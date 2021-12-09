import logging
import enum


logger = logging.getLogger(__name__)


class InputMode(enum.Enum):
    append = "append"
    merge = "merge"
