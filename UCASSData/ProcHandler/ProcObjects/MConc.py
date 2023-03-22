from .__Proc import Proc


class MConc(Proc):

    def __proc(self, **kwargs):
        data = self.__getcols([])
