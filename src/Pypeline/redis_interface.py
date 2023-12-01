from typing import List
from datetime import datetime
import json

import redis

from .dataclasses import ServiceIdentifier, ProcessIdentifier, ServiceStatus, ProcessState, ProcessStatus, JobEventMessage, ProcessNoteMessage


class _RedisStatusInterface:
    def __init__(self, redis_address: str, *channel_subscriptions: List[str], **redis_kwargs):
        redis_kwargs["decode_responses"] = redis_kwargs.get("decode_responses", True)
        ignore_subscribe_messages = redis_kwargs.pop("ignore_subscribe_messages", True)
        self.redis_obj = redis.Redis(**redis_kwargs)
        self.redis_address = redis_address
        self.rh_status = f"pypeline://{self.redis_address}/status"
        self.rc_subscriptions = {}
        for chan in channel_subscriptions:
            pubsub = self.redis_obj.pubsub(ignore_subscribe_messages=ignore_subscribe_messages)
            pubsub.subscribe(chan)
            self.rc_subscriptions[chan] = pubsub

    def __del__(self):
        if not hasattr(self, "rc_subscriptions"):
            # not initialised properly
            return
        for chan, pubsub in self.rc_subscriptions.items():
            pubsub.unsubscribe()

    def __setitem__(self, key, value):
        return self.redis_obj.hset(self.rh_status, key, value)

    def set(self, key, value, assertion_tuple=(True, "")):
        assert assertion_tuple[0], assertion_tuple[1]
        return self.__setitem__(key, value)

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
    
    def publish(self, channel, message, assertion_tuple=(True, "")):
        assert assertion_tuple[0], assertion_tuple[1]
        return self.redis_obj.publish(f"pypeline://{self.redis_address}/{channel}", message)



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

    job_event_message: JobEventMessage = property(
        fget=None,
        fset=lambda self, value: self.publish(
            "jobs",
            str(value),
            assertion_tuple=(
                isinstance(value, JobEventMessage),
                "Must be instance of `JobEventMessage`"
            )
        ),
        fdel=None,
        doc="Publishes `JobEventMessage` under the 'jobs' channel."
    )

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

    processes: List[ProcessState] = property(
        fget=lambda self: {
            ProcessIdentifier.from_str(k): ProcessState(*v)
            for k, v in json.loads(self.__getitem__("PROCESSES")).items()
        },
        fset=lambda self, value: self.set(
            "PROCESSES",
            json.dumps({
                str(self.id.process_identifier(i)): v
                for i, v in enumerate(value)
            }),
            assertion_tuple=(
                all(map(
                    lambda v: isinstance(v, ProcessState),
                    value
                )),
                f"Must be a dict(Identifier, ProcessState)"
            )
        ),
        fdel=None,
        doc="A map of process identifiers to state-timestamps."
    )

    process_note_message: ProcessNoteMessage = property(
        fget=None,
        fset=lambda self, value: self.publish(
            "notes",
            str(value),
            assertion_tuple=(
                isinstance(value, ProcessNoteMessage),
                "Must be instance of `ProcessNoteMessage`"
            )
        ),
        fdel=None,
        doc="Publishes `ProcessNoteMessage` under the 'notes' channel."
    )

    process_status: ProcessStatus = property(
        fget=lambda self: ProcessStatus.from_str(self.__getitem__("STATUS")),
        fset=lambda self, value: self.set(
            f"STATUS:{value.process_id}",
            str(value),
            assertion_tuple=(isinstance(value, ProcessStatus), f"Must be instance of ProcessStatus")
        ),
        fdel=None,
        doc="Status object."
    )
