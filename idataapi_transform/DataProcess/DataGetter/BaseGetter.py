import abc
from ..Meta.BaseDataProcess import BaseDataProcess


class BaseGetter(BaseDataProcess, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        """
        :param config
            config contains attribute:
                source: where to read data
                per_limit: return at most per_limit data each time
                max_limit: return at most max_limit data total
        """
        pass

    @abc.abstractmethod
    def __aiter__(self):
        return self

    @abc.abstractmethod
    async def __anext__(self):
        pass
