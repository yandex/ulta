from dataclasses import dataclass
from enum import IntEnum


class AgentOrigin(IntEnum):
    UNKNOWN = 0
    COMPUTE_LT_CREATED = 1
    EXTERNAL = 2


@dataclass
class AgentInfo:
    id: str
    name: str
    version: str
    origin: AgentOrigin
    folder_id: str

    def is_external(self) -> bool:
        return self.origin == AgentOrigin.EXTERNAL

    def is_anonymous_external_agent(self) -> bool:
        return self.is_external() and not bool(self.name) and self.folder_id

    def is_persistent_external_agent(self) -> bool:
        return bool(self.is_external() and self.name and self.folder_id)
