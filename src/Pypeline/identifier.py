from dataclasses import dataclass

@dataclass
class Identifier:
    hostname: str
    enumeration: int
    process_enumeration: int

    def __str__(self):
        return f"{self.hostname}:{self.enumeration}.{self.process_enumeration}"
    
    def __hash__(self):
        return hash(str(self))