from enum import Enum

class ModelState(str, Enum):
    IGNORE = "IGNORE"
    MONITOR = "MONITOR"
    TRACK = "TRACK"
    TERMINAL = "TERMINAL"
