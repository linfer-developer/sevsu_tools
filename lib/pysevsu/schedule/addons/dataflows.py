# data_manager.py

from typing import Callable
from database.tables import *
from database.interface import *
from utilites import callbacks as callbacker


async def async_start_generation(generator: object, callbacks: Callable):
    async for item in generator():
        if item:
            callbacker.start(*callbacks)
