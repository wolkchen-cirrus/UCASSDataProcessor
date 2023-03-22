from .__Proc import Proc


class Conc(Proc):

    def __proc(self, **kwargs):
        try:
            t = kwargs["t"]
        except KeyError:
            raise ValueError("Set t for conc proc")
        match t:
            case "number":
                return 0
            case "mass":
                return 0
            case "volume":
                return 0
            case _:
                raise ValueError(f"{t} is invalid option")
