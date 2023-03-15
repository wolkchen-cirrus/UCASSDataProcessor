from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from .__Proc import Proc


class Conc(Proc):
    def init(self, di: md):
        return self.__proc()

    def __proc(self):
        return
