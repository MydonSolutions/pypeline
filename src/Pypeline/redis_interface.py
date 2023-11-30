from typing import List, Optional
from datetime import datetime
import json
import re

import redis

from pydantic import BaseModel

from .identifier import ServiceIdentifier, ProcessIdentifier

class ServiceStatus(BaseModel):
    workers_busy_count: int
    workers_total_count: int
    job_queue: List

    def __str__(self) -> str:
        return f"{self.workers_busy_count}/{self.workers_total_count} ({len(self.job_queue)} queued)"

    @staticmethod
    def from_str(s) -> "ServiceStatus":
        m = re.match(r"(?P<workers_busy_count>\d+)/(?P<workers_total_count>\d+) \((?P<jobs_queued_count>\d+) queued\)", s)
        if m is None:
            raise ValueError(f"Incompatible string: '{s}'")
        return ServiceStatus(
            workers_busy_count=int(m.group("workers_busy_count")),
            workers_total_count=int(m.group("workers_total_count")),
            jobs_queued_count=int(m.group("jobs_queued_count")),
        )

class ProcessStatus(BaseModel):
    timestamp_last_stage: float
    last_stage: str

    def __str__(self) -> str:
        return f"{self.timestamp_last_stage}-{self.last_stage}"

    @staticmethod
    def from_str(s) -> "ProcessStatus":
        m = re.match(r"(?P<timestamp>\d+(\.\d+))-(?P<stage>\w+)", s)
        if m is None:
            raise ValueError(f"Incompatible string: '{s}'")
        return ProcessStatus(
            timestamp_last_stage=m.group("timestamp"),
            last_stage=m.group("stage")
        )

class ProcessStateTimestamps(BaseModel):
    start: Optional[float]
    finish: Optional[float]
    error: Optional[float]
    
    def __str__(self):
        return self.model_dump_json()


class _RedisStatusInterface:
    def __init__(self, redis_address: str, *channel_subscriptions: List[str], **redis_kwargs):
        redis_kwargs["decode_responses"] = redis_kwargs.get("decode_responses", True)
        ignore_subscribe_messages = redis_kwargs.pop("ignore_subscribe_messages", True)
        self.redis_obj = redis.Redis(**redis_kwargs)
        self.rh_status = f"pypeline://{redis_address}/status"
        self.rc_subscriptions = {}
        for chan in channel_subscriptions:
            pubsub = self.redis_obj.pubsub(ignore_subscribe_messages=ignore_subscribe_messages)
            pubsub.subscribe(chan)
            self.rc_subscriptions[chan] = pubsub

    def __del__(self):
        for chan, pubsub in self.rc_subscriptions.items():
            pubsub.unsubscribe()

    def __setitem__(self, key, value):
        self.redis_obj.hset(self.rh_status, key, value)

    def set(self, key, value, assertion_tuple=(True, "")):
        assert assertion_tuple[0], assertion_tuple[1]
        self.__setitem__(key, value)

    def __getitem__(self, key):
        v = self.redis_obj.hget(self.rh_status, key)
        if v is None:
            raise KeyError(key)
        return v

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def get_all(self):
        return self.redis_obj.hgetall(self.rh_status)

    def clear(self, exclusion_list=[]):
        all_keys = self.redis_obj.hkeys(self.rh_status)
        keys_to_clear = [key for key in all_keys if key not in exclusion_list]
        if len(keys_to_clear) > 0:
            self.redis_obj.hdel(self.rh_status, *keys_to_clear)


class RedisServiceInterface(_RedisStatusInterface):
    REDIS_HASH_KEYS = ["#CONTEXT", "#CONTEXTENV", "#STAGES", "STATUS", "PULSE", "PROCESSES"]

    def __init__(self, id: ServiceIdentifier, **redis_kwargs):
        if not isinstance(id, ServiceIdentifier):
            raise ValueError("Interface ID must be an instance of ProcessIdentifier")
        super().__init__(id.redis_address(), "pypeline:///set", **redis_kwargs)
        self.id = id


    def process_broadcast_set_messages(self, timeout_s):
        message = self.rc_subscriptions["pypeline:///set"].get_message(timeout=timeout_s)
        if message is None:
            return False

        if isinstance(message.get("data"), bytes):
            message["data"]  = message["data"].decode()
        # TODO rather implement redis_obj.hset(, mapping={})
        for keyvaluestr in message["data"].split("\n"):
            parts = keyvaluestr.split("=")
            self.set(parts[0], '='.join(parts[1:]))
        return True


    context: str = property(
        fget=lambda self: self.__getitem__("#CONTEXT"),
        fset=lambda self, value: self.__setitem__("#CONTEXT", value),
        fdel=None,
        doc="Name of the contextual stage."
    )

    context_environment: str = property(
        fget=lambda self: self.get("#CONTEXTENV", None),
        fset=lambda self, value: self.__setitem__("#CONTEXTENV", value),
        fdel=None,
        doc="Optional environment string argument for the context's run function."
    )

    stages: List[str] = property(
        fget=lambda self: self.__getitem__("#STAGES").split(" "),
        fset=lambda self, value: self.__setitem__("#STAGES", " ".join(value)),
        fdel=None,
        doc="Space delimited list of stages that follow the contextual stage."
    )

    pulse: datetime = property(
        fget=lambda self: datetime.strptime(self.__getitem__("PULSE"), "%Y/%m/%d %H:%M:%S"),
        fset=lambda self, value: self.__setitem__("PULSE", value.strftime("%Y/%m/%d %H:%M:%S")),
        fdel=None,
        doc="Heartbeat datetime that is updated during operation to indicate that the service is alive."
    )

    status: ServiceStatus = property(
        fget=lambda self: ServiceStatus.from_str(self.__getitem__("STATUS")),
        fset=lambda self, value: self.set(
            "STATUS",
            str(value),
            assertion_tuple=(isinstance(value, ServiceStatus), f"Must be instance of Status")
        ),
        fdel=None,
        doc="Status object."
    )

    processes: List[ProcessStateTimestamps] = property(
        fget=lambda self: {
            ProcessIdentifier.from_str(k): ProcessStateTimestamps(*v)
            for k, v in json.loads(self.__getitem__("PROCESSES")).items()
        },
        fset=lambda self, value: self.set(
            "PROCESSES",
            json.dumps({
                str(self.id.process_identifier(i)): v.model_dump()
                for i, v in enumerate(value)
            }),
            assertion_tuple=(
                all(map(
                    lambda v: isinstance(v, ProcessStateTimestamps),
                    value
                )),
                f"Must be a dict(Identifier, ProcessStateTimestamps)"
            )
        ),
        fdel=None,
        doc="A map of process identifiers to state-timestamps."
    )

class RedisProcessInterface(_RedisStatusInterface):
    def __init__(self, id: ProcessIdentifier, **redis_kwargs):
        if not isinstance(id, ProcessIdentifier):
            raise ValueError("Interface ID must be an instance of ProcessIdentifier")
        super().__init__(id.redis_address(), **redis_kwargs)

    status: ProcessStatus = property(
        fget=lambda self: ProcessStatus.from_str(self.__getitem__("STATUS")),
        fset=lambda self, value: self.set(
            "STATUS",
            str(value),
            assertion_tuple=(isinstance(value, ProcessStatus), f"Must be instance of ProcessStatus")
        ),
        fdel=None,
        doc="Status object."
    )
