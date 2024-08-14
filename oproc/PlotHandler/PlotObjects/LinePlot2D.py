from .__Plot import Plot
import numpy as np
from ... import newprint


# Redefining print function with timestamp
print = newprint()


class LinePlot2D(Plot):

    #TODO: plot spec and ivars could be different
    #TODO: plot spec can include rows, cols, and number of plots on each
    #TODO: maybe get rid of setup, and pass plot spec into __init__()

#    def setup(self):
#        try:
#            self.ivars = [self.args['x'], self.args['y']]
#        except KeyError:
#            raise RuntimeError('x and y must be passed as kwargs into \
#                               LinePlot2D object :)')

    def plot(self, ivars):
        self.ivars = ivars

        return self.handle

    def __repr__(self):
        return "LinePlot2D"
