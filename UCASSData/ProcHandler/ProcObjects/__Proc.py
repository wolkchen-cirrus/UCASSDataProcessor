from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from datetime import datetime as dt


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({dt.now()})', *args, **kwargs)


print = timestamped_print


class Proc(object):
    """
    Base processor object. User-defined processes must be subclasses of this
    object.
    """

    def __int__(self, di: md):
        self.__di = None
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, 'init'):
                cls.init(self, di)

    def init(self, di: md):
        self.di = di
        return self.__proc()

    def __proc(self):
        print("Undefined proc, returning input")
        return self.di

    @property
    def di(self):
        return self.__di

    @di.setter
    def di(self, val):
        self.__di = val
