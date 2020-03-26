import re
import logging

PATTERN = re.compile(r"^([^=]+?=[^=]+?_){3,}.*\.mp4$", re.IGNORECASE)


def is_file_match(filename):
    logging.debug(f"filename: {filename}")
    r = PATTERN.match(filename) is not None
    logging.debug(f"done: {r} {filename}")
    return r
