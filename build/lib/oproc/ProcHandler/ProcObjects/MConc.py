from .__Proc import Proc


class MConc(Proc):

    def __proc(self, **kwargs):
        col_data = self.__getcols(["C#, Period"])

    def __repr__(self):
        return "MConc"
