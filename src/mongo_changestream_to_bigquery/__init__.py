import logging
import sys

logging.basicConfig(
    stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s"
)
logging.getLogger("botocore").setLevel(logging.WARNING)
