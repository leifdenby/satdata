"""
Utilities to create (approximate) square tiles from lat/lon satelite data
"""
import cartopy.crs as ccrs
import xesmf
import xarray as xr
import numpy as np
from scipy.constants import pi
import shapely.geometry as geom

import itertools
import warnings

from .utils import create_true_color_img


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

    def _crop_input(self, da, pad_pct=0.1):
        xs, ys, _ = da.crs.transform_points(ccrs.PlateCarree(),
                                            *self.get_bounds().T
                                           ).T
        x_min, x_max = np.min(xs), np.max(xs)
        y_min, y_max = np.min(ys), np.max(ys)
        lx = x_max - x_min
        ly = y_max - y_min

        x_min -= pad_pct*lx
        y_min -= pad_pct*ly
        x_max += pad_pct*lx
        y_max += pad_pct*ly

        if da.x[0] > da.x[-1]:
            x_slice = slice(x_max, x_min)
        else:
            x_slice = slice(x_min, x_max)

        if da.y[0] > da.y[-1]:
            y_slice = slice(y_max, y_min)
        else:
            y_slice = slice(y_min, y_max)

        return da.sel(x=x_slice, y=y_slice)

    def resample(self, da, N, method='bilinear', crop_pad_pct=0.1):
        """
        Resample a xarray DataArray onto this tile with grid made of NxN points
        """
        da = self._crop_input(da=da, pad_pct=crop_pad_pct)

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

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            regridder = xesmf.Regridder(filename=regridder_weights_fn,
                reuse_weights=True, ds_in=old_grid, ds_out=new_grid,
                method=method, 
            )

        da_resampled = regridder(da)
        da_resampled['x'] = new_grid.x
        da_resampled['y'] = new_grid.y

        return da_resampled

    def create_true_color_img(self, das_channels, resampling_N):
        das_channels_resampled = [self.resample(da, N=resampling_N)
                                  for da in das_channels]
        return create_true_color_img(das_channels=das_channels_resampled)


def triplet_generator(target_channels, tile_size, tiling_bbox, tile_N,
                      distant_channels=None, neigh_dist_scaling=1.0,
                      distant_dist_scaling=10.):
    # generate (lat, lon) locations inside tiling_box

    # XXX: this is a really poor approximation to degrees to meters, should use
    # Haversine formula or something like it
    deg_to_m = 100e3
    tile_size_deg = tile_size/deg_to_m
    
    def _generate_latlon():
        (lon_min, lat_min), (lon_max, lat_max) = tiling_bbox
        
        lat = lat_min + (lat_max - lat_min)*np.random.random()
        lon = lon_min + (lon_max - lon_min)*np.random.random()

        return (lon, lat)

    def _perturb_loc(loc, scaling):
        theta = 2*pi*np.random.random()
        r = scaling*tile_size_deg*np.random.normal(loc=1.0, scale=0.1)

        dlon = r*np.cos(theta)
        dlat = r*np.sin(theta)

        return (loc[0] + dlon, loc[1] + dlat)


    anchor_loc = _generate_latlon()
    neighbor_loc = _perturb_loc(anchor_loc, scaling=neigh_dist_scaling)

    if distant_channels is None:
        dist_loc = _generate_latlon()
    else:
        dist_loc = _perturb_loc(anchor_loc, scaling=distant_dist_scaling)

    locs = [anchor_loc, neighbor_loc, dist_loc]

    tiles = [
        Tile(lat0=lat, lon0=lon, size=tile_size)
        for (lon, lat) in locs
    ]

    channel_sets = [target_channels, target_channels]
    if distant_channels is None:
        channel_sets.append(target_channels)
    else:
        channel_sets.append(distant_channels)

    return [
        tile.create_true_color_img(das_channels, resampling_N=tile_N)
        for (tile, das_channels) in zip(tiles, channel_sets)
    ]
