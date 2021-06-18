"""Exceptions used by pipelinewise-target-vertica"""

class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""
