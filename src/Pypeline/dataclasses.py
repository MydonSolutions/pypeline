from typing import Dict, List, Optional
import re
from enum import Enum

from pydantic import BaseModel

### Process Classes

class ProcessIdentifier(BaseModel):
    hostname: str
    enumeration: int
    process_enumeration: Optional[int]

    def __init__(self, hostname: str, enumeration: int, process_enumeration: int):
        super().__init__(
            hostname=hostname,
            enumeration=enumeration,
            process_enumeration=process_enumeration
        )

    @staticmethod
    def from_str(s: str) -> "ProcessIdentifier":
        m = re.match(r"(?P<hostname>\w+):(?P<enumeration>\d+)\.(?P<process_enumeration>\d+)", s)
        if m is None:
            raise ValueError(f"Inappropriate string: '{s}'")
        return ProcessIdentifier(
            hostname=m.group("hostname"),
            enumeration=int(m.group("enumeration")),
            process_enumeration=int(m.group("process_enumeration"))
        )
    
    def redis_address(self) -> str:
        prefix = f"{self.hostname}/{self.enumeration}/{self.process_enumeration}"
        return prefix

    def __str__(self):
        prefix = f"{self.hostname}:{self.enumeration}.{self.process_enumeration}"
        return prefix

    def __hash__(self):
        return hash(self.__str__())


class StageTimestamp(BaseModel):
    name: str
    start: float
    end: Optional[float]


class ProcessStatus(BaseModel):
    job_id: int
    process_id: int
    stage_timestamps: List[StageTimestamp]

    def __str__(self) -> str:
        return self.model_dump_json()

class ProcessState(str, Enum):
    Idle = "idle"
    Busy = "busy"
    Finished = "finished"
    Errored = "errored"


class ProcessNote(str, Enum):
    Start = "Start"
    StageStart = "Stage Start"
    StageFinish = "Stage Finish"
    StageError = "Stage Error"
    Finish = "Finish"
    Error = "Error"

    @staticmethod
    def string(note: "ProcessNote") -> str:
        return note.value

class ProcessNoteMessage(BaseModel):
    job_id: int
    process_id: int
    process_note: ProcessNote
    stage_name: Optional[str]
    error_message: Optional[str]

    def __str__(self) -> str:
        return self.model_dump_json()

### Job classes

class JobParameters(BaseModel):
    job_id: int
    redis_kvcache: Dict[str, str]
    stage_outputs: Dict[str, List] # {stage_name::string: output::list}
    stage_list: List[str]
    dehydrated_context: tuple # context.dehydrate()


class JobEvent(str, Enum):
    Drop = "drop"
    Skip = "skip"
    Queue = "queue"


class JobEventMessage(BaseModel):
    action: JobEvent
    job_parameters: JobParameters

    def __str__(self) -> str:
        return self.model_dump_json()

### Service entrypoint classes


class ServiceIdentifier(BaseModel):
    hostname: str
    enumeration: int

    def __init__(self, hostname: str, enumeration: int):
        super().__init__(
            hostname=hostname,
            enumeration=enumeration
        )

    @staticmethod
    def from_str(s: str) -> "ServiceIdentifier":
        m = re.match(r"(?P<hostname>\w+):(?P<enumeration>\d+)", s)
        if m is None:
            raise ValueError(f"Inappropriate string: '{s}'")
        return ServiceIdentifier(
            hostname=m.group("hostname"),
            enumeration=int(m.group("enumeration")),
        )
    
    def process_identifier(self, process_enumeration: int) -> ProcessIdentifier:
        return ProcessIdentifier(
            self.hostname,
            self.enumeration,
            process_enumeration
        )

    def redis_address(self) -> str:
        return f"{self.hostname}/{self.enumeration}"

    def __str__(self):
        return f"{self.hostname}:{self.enumeration}"

    def __hash__(self):
        return hash(self.__str__())


class ServiceStatus(BaseModel):
    workers_busy_count: int
    workers_total_count: int
    process_job_queue: List

    def __str__(self) -> str:
        return f"{self.workers_busy_count}/{self.workers_total_count} ({len(self.process_job_queue)} queued)"

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
