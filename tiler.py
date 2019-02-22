"""
Utilities to create (approximate) square tiles from lat/lon satelite data
"""
import cartopy.crs as ccrs
import xesmf
import xarray as xr
import numpy as np
import shapely.geometry as geom

import itertools


class Tile():
    def __init__(self, lat0, lon0, size):
        self.lat0 = lat0
        self.lon0 = lon0
        self.size = size

    def get_bounds(self):
        """
        The the lat/lon bounds of the tile. First calculates the approximate
        lat/lon distance as if the tile was centered on the equator and then
        uses a rotated pole projection to move the title
        """
        ddeg = self._get_approximate_equator_latlon_dist()

        corners_dir = list(itertools.product([1,-1], [1,-1]))
        corners_dir.insert(0, corners_dir.pop(2))

        corners = ddeg*np.array(corners_dir)

        return self._transform_from_equator(lon=corners[:,0], lat=corners[:,1])

    def _transform_from_equator(self, lon, lat):
        p = ccrs.RotatedPole(
            pole_latitude=90 + self.lat0,
            pole_longitude=self.lon0,
            central_rotated_longitude=-180.
        )

        return ccrs.PlateCarree().transform_points(p, lon, lat)[...,:2]

    def _get_approximate_equator_latlon_dist(self):
        # approximate lat/lon distance
        r = 6371e3 # [m]
        return np.arcsin(self.size/2./r)*180./3.14

    def get_outline_shape(self):
        """return a shapely shape valid for plotting"""

        return geom.Polygon(self.get_bounds())

    def get_grid(self, N):
        """
        Get an xarray Dataset containing the new lat/lon grid points with their
        position in meters
        """
        ddeg = self._get_approximate_equator_latlon_dist()

        lat_eq_ = lon_eq_ = np.linspace(-ddeg, ddeg, N)
        lon_eq, lat_eq = np.meshgrid(lon_eq_, lat_eq_)

        pts = self._transform_from_equator(lon=lon_eq, lat=lat_eq)

        x = xr.DataArray(
            np.arange(-self.size/2., self.size/2, self.size/N),
            attrs=dict(longname='distance', units='m'),
            dims=('x',)
        )
        y = xr.DataArray(
            np.arange(-self.size/2., self.size/2, self.size/N),
            attrs=dict(longname='distance', units='m'),
            dims=('y',)
        )

        ds = xr.Dataset(coords=dict(x=x, y=y))

        ds['lon'] = (('x', 'y'), pts[...,0])
        ds['lat'] = (('x', 'y'), pts[...,1])

        return ds

    def resample(self, da, N, method='bilinear'):
        """
        Resample a xarray DataArray onto this tile with grid made of NxN points
        """
        old_grid = xr.Dataset(coords=da.coords)

        if not hasattr(da, 'crs'):
            raise Exception("The provided DataArray doesn't have a "
                            "projection provided. Please set the `crs` "
                            "attribute to contain a cartopy projection")

        latlon_old = ccrs.PlateCarree().transform_points(
            da.crs, *np.meshgrid(da.x.values, da.y.values),
        )[:,:,:2]

        old_grid['lat'] = (('y', 'x'), latlon_old[...,1])
        old_grid['lon'] = (('y', 'x'), latlon_old[...,0])

        new_grid = self.get_grid(N=N)

        Nx_in, Ny_in = da.x.shape[0], da.y.shape[0]
        Nx_out, Ny_out = N, N

        regridder_weights_fn = "{method}_{Ny_in}x{Nx_in}_{Ny_out}x{Nx_out}"\
                               "__{lat0}_{lon0}.nc".format(
            lon0=self.lon0, lat0=self.lat0, method=method, Ny_in=Ny_in,
            Nx_in=Nx_in, Nx_out=Nx_out, Ny_out=Ny_out,
        )

        regridder = xesmf.Regridder(filename=regridder_weights_fn,
            reuse_weights=True, ds_in=old_grid, ds_out=new_grid,
            method=method,
        )

        da_resampled = regridder(da)
        da_resampled['x'] = new_grid.x
        da_resampled['y'] = new_grid.y

        return da_resampled
