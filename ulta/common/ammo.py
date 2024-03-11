import os
from dataclasses import dataclass


@dataclass
class Ammo:
    name: str
    path: str

    @property
    def abs_path(self):
        return os.path.abspath(self.path)
