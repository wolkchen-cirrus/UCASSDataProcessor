from .__Plot import Plot
import numpy as np
from ... import newprint
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
from matplotlib import pyplot as plt


# Redefining print function with timestamp
print = newprint()


class OSMTracePlot(Plot):


    def init_fig(self):
        pass

    def init_plot(self):
        mask = []
        try:
            mask = self.args['mask']
        except KeyError:
            pass
        request = cimgt.OSM()
        self.ax = plt.axes(projection=request.crs)
        self.fig = self.ax.get_figure()
        data = self.get_ivars()
        pss = self.plot_spec.pss
        try:
            ex = self.args['Extent']
            zl = self.args['Zoom']
        except KeyError:
            raise ValueError('\"extent\" and \"Zoom\" must be passed as a \
                             kwargs >:3')
        pss_i = pss[0, 0]
        labels = pss_i.data
        self.ax.set_extent(ex)
        self.ax.add_image(request, zl)
        if mask:
            for m in mask:
                data['Lng'] = self.apply_plot_mask(m, data['Lng'])
                data['Lat'] = self.apply_plot_mask(m, data['Lat'])
        self.ax.plot(data['Lng'], data['Lat'], transform=ccrs.PlateCarree())

        lhs = 0.1*(ex[1]-ex[0])+ex[0]
        bhs = 0.1*(ex[3]-ex[2])+ex[2]
        self.ax.plot([lhs, lhs+0.01], [bhs, bhs], transform=ccrs.PlateCarree())
#        lhsa = (lhs+lhs+0.01)/2
#        self.ax.annotate('1 km', xy=(lhsa, bhs), xytext=(lhsa, bhs))

#        self.fig.suptitle(self.__repr__())




