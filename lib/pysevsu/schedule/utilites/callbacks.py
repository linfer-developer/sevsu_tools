import functools
from typing import Any
from typing import Optional
from typing import Callable
from typing import Final
from typing import Tuple


CALLBACK_TYPES: Tuple[str] = ("parent", "child")

def create(*args, **kwargs):
    def decorator(func):
        if (kwargs["type_"] in CALLBACK_TYPES):
            func.type = kwargs["type_"]
        else: 
            raise AttributeError(
                "One of the decorator attributes was passed incorrectly."
            )
        return func
    return decorator

def start(*args: Optional[Callable], **kwargs: Optional[Any]):
    for callback in args:
        if callback.type == "parent":
            callback_index = args.index(callback)
            args = args[:callback_index] + args[callback_index+1:]
            callback(*args, **kwargs)
        if callback.type == "child":
            callback(*args, **kwargs)
        else:
            raise RuntimeError(
                "The create decorator parameters were passed incorrectly/not "\
                "passed/the decorator was not created."
            )
