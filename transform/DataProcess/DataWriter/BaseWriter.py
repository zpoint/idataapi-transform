import abc
from ..Meta.BaseDataProcess import BaseDataProcess


class BaseWriter(BaseDataProcess, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        """
        :param config
        """
        pass

    @abc.abstractmethod
    async def write(self, responses):
        pass

    @abc.abstractmethod
    def __enter__(self):
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
