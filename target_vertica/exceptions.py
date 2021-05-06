"""Exceptions used by pipelinewise-target-vertica"""
from vertica_python.errors import CopyRejected


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""
    pass


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""
    pass
