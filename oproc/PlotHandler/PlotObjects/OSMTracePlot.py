from .__Plot import Plot
import numpy as np
from ... import newprint
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt


# Redefining print function with timestamp
print = newprint()


class OSMTracePlot(Plot):


    def init_fig(self):
        if self.__fig:
            print("figure object exists")
            return
        else:
            fig, ax = plt.subplots(self.shape[0], self.shape[1],\
                                        layout='constrained',
                                        projection=request.crs
                                  )
            self.__fig = fig
            try:
                self.__ax = np.matrix(ax)
            except ValueError:
                self.__ax = np.matrix([ax])


    def init_plot(self):
        mask = []
        try:
            mask = self.args['mask']
        except KeyError:
            pass
        data = self.get_ivars()
        pss = self.plot_spec.pss

        request = cimgt.OSM()
        try:
            extent = self.args['Extent']
            zl = self.args['Zoom']
        except KeyError:
            raise ValueError('\"extent\" and \"Zoom\" must be passed as a \
                             kwargs >:3')

        for r in range(self.shape[0]):
            for c in range(self.shape[1]):
                pss_i = pss[r, c]
                labels = pss_i.data
                n_lines = pss_i.n_lines
                n_dims = pss_i.n_dims
                for l in range(n_lines):
                    if mask:
                        for m in mask:
                            data['Lat'] = self.apply_plot_mask(m, data['Lat'])
                            data['Lon'] = self.apply_plot_mask(m, data['Lon'])
                    else:
                        pass
                    if n_dims == 2:
                        self.__ax[r, c].set_extent(extent)
                        self.__ax[r, c].add_image(request, zl)
                        self.__ax[r, c].plot(data['Lon'], data_i['Lat'],
                                             transform=ccrs.PlateCarree())
                    else:
                        raise NotImplementedError("Not currently implemented \
                                                  for dimensions other than 2")

                    self.__ax[r,c].set_xlabel(self.get_disp_name(labels[l,0]))
                    self.__ax[r,c].set_ylabel(self.get_disp_name(labels[l,1]))

        self.__fig.suptitle(self.__repr__())




