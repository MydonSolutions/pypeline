import re
from pydantic import BaseModel
from typing import Optional


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
