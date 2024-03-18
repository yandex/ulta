import re
from typing_extensions import Annotated
from pydantic.functional_validators import AfterValidator


def str_len(*args):
    min_len, max_len = None, None
    try:
        iterator = iter(args)
        max_len = next(iterator)
        min_len, max_len = max_len, next(iterator)
    except StopIteration:
        pass

    def validator(v: str | None):
        if v is not None:
            if min_len is not None and len(v) < min_len:
                raise AssertionError(f'{v} must be at least {min_len} chars long')
            if max_len is not None and len(v) > max_len:
                raise AssertionError(f'{v} must be at most {max_len} chars long')
        return v

    return validator


def re_match(pattern):
    regexp = re.compile(pattern)

    def validator(v: str | None):
        if v is not None and regexp.fullmatch(v) is None:
            raise AssertionError(f"{v} doesn't match pattern {pattern}")
        return v

    return validator


def dict_size(size: int):
    def validator(v: dict | None):
        if v is not None and len(v) > size:
            raise AssertionError(f'may have at most {v} values')
        return v

    return validator


LabelKey = Annotated[str, AfterValidator(str_len(1, 63)), AfterValidator(re_match('[a-z][-_0-9a-z]*'))]
LabelValue = Annotated[str, AfterValidator(str_len(1, 63)), AfterValidator(re_match('[-_0-9a-z]*'))]
