import re

PATTERN = re.compile(r"^Channel=(.+?=.+?)+\.mp4$")


def is_file_match(filename):
    return PATTERN.match(filename) is not None
