import logging
import subprocess
from typing import Any, Dict, List, Optional, Union
import re
from enum import Enum

from pydantic import BaseModel, ConfigDict

### Process Classes


class ProcessIdentifier(BaseModel):
    hostname: str
    enumeration: int
    process_enumeration: Optional[int]

    def __init__(self, hostname: str, enumeration: int, process_enumeration: int):
        super().__init__(
            hostname=hostname,
            enumeration=enumeration,
            process_enumeration=process_enumeration,
        )

    @staticmethod
    def from_str(s: str) -> "ProcessIdentifier":
        m = re.match(
            r"(?P<hostname>\w+):(?P<enumeration>\d+)\.(?P<process_enumeration>\d+)", s
        )
        if m is None:
            raise ValueError(f"Inappropriate string: '{s}'")
        return ProcessIdentifier(
            hostname=m.group("hostname"),
            enumeration=int(m.group("enumeration")),
            process_enumeration=int(m.group("process_enumeration")),
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
    HandlingStageException = "exception"
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
    context_name: str
    context_output: list
    context_dehydrated: Union[tuple, dict, list]  # context.dehydrate()
    stage_list: List[str]


class JobEvent(str, Enum):
    Drop = "drop"
    Skip = "skip"
    Queue = "queue"


class JobEventMessage(BaseModel):
    event: JobEvent
    job_parameters: JobParameters

    def __str__(self) -> str:
        return self.model_dump_json()


class JobProgress(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    imported_modules: bool = False

    stage_index: int = 0
    stage_list: List[str] = []

    stage_map_envvar: Dict[str, Optional[str]] = {}
    stage_map_input_templates: Dict[str, List[str]] = {}
    stage_map_input_templateindices: Dict[str, int] = {}
    stage_map_inputs: Dict[str, Union[List[List[str]], List[str], bool]] = {}
    stage_map_inputindices: Dict[str, int] = {}
    stage_map_lastinput: Dict[str, Union[List[List[str]], List[str], bool, None]] = {}
    stage_map_args: Dict[str, Optional[List[str]]] = {}
    stage_map_argindices: Dict[str, int] = {}
    stage_map_popened: Dict[str, subprocess.Popen] = {}
    stage_map_outputs: Dict[str, List[Any]] = {}

    def __init__(self, job_parameters: JobParameters):
        super().__init__()
        self.stage_list.extend(job_parameters.stage_list)
        self.stage_map_outputs[job_parameters.context_name] = (
            job_parameters.context_output
        )

    def stage_name(self) -> str:
        return self.stage_list[self.stage_index]

    def increment(self, logger: logging.Logger):
        stage_name = self.stage_name()

        # Increment through inputs, overflow increment through arguments
        self.stage_map_inputindices[stage_name] += 1
        if self.stage_map_inputindices[stage_name] >= len(
            self.stage_map_inputs[stage_name]
        ):
            self.stage_map_inputindices[stage_name] = 0
            self.stage_map_input_templateindices[stage_name] += 1
            if self.stage_map_input_templateindices[stage_name] >= len(
                self.stage_map_input_templates[stage_name]
            ):
                self.stage_map_input_templateindices[stage_name] = 0
                self.stage_map_argindices[stage_name] += 1

        # Proceed to next process or...
        if self.stage_index + 1 < len(self.stage_list):
            logger.debug("Next process")

            self.stage_index += 1
            stage_name = self.stage_list[self.stage_index]
            if stage_name[0] == "*":
                stage_name = stage_name[1:]
            self.stage_map_input_templateindices[stage_name] = 0
            self.stage_map_inputindices[stage_name] = 0
            self.stage_map_argindices[stage_name] = 0
        else:  # ... rewind to the closest next novel process (argumentindices indicate exhausted permutations)
            logger.debug(f"Rewinding after {stage_name}")
            while self.stage_index >= 0 and self.stage_map_argindices[
                stage_name
            ] >= len(self.stage_map_args[stage_name]):
                progress_str = self.get_progress_str()
                logger.debug(progress_str)

                self.stage_index -= 1
                stage_name = self.stage_list[self.stage_index]
                if stage_name[0] == "*":
                    stage_name = stage_name[1:]

            # Break if there are no novel process argument-input permutations
            if self.stage_index < 0:
                raise StopIteration

    def get_progress_str(self) -> str:
        stage_name = self.stage_name()
        return (
            f"{stage_name}: "
            + f"input_templateindex {self.stage_map_input_templateindices[stage_name]+1}/{len(self.stage_map_input_templates[stage_name])}, "
            + f"inputindex {self.stage_map_inputindices[stage_name] + 1}/{len(self.stage_map_inputs[stage_name])}, "
            + f"argindex {self.stage_map_argindices[stage_name]}/{len(self.stage_map_args[stage_name])}"
        )


### Service entrypoint classes


class ServiceIdentifier(BaseModel):
    hostname: str
    enumeration: int

    def __init__(self, hostname: str, enumeration: int):
        super().__init__(hostname=hostname, enumeration=enumeration)

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
        return ProcessIdentifier(self.hostname, self.enumeration, process_enumeration)

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
        m = re.match(
            r"(?P<workers_busy_count>\d+)/(?P<workers_total_count>\d+) \((?P<jobs_queued_count>\d+) queued\)",
            s,
        )
        if m is None:
            raise ValueError(f"Incompatible string: '{s}'")
        return ServiceStatus(
            workers_busy_count=int(m.group("workers_busy_count")),
            workers_total_count=int(m.group("workers_total_count")),
            jobs_queued_count=int(m.group("jobs_queued_count")),
        )
