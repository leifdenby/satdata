import os
import random

from tqdm import tqdm
import xarray as xr
import cartopy.crs as ccrs

import numpy as np
import time
import yaml

from . import tiler
from .multiprocessing import parallel


def load_channels(fns):
    pass


def find_dataset_filenames(times, dt_max, cli, channels=[1,2,3]):
    """
    query API to find datasets with each required channel
    """
    def get_channel_file(t, channel):
        return cli.query(time=t, region='F', debug=False, channel=channel,
                         dt_max=dt_max)

    filenames = []
    for t in times:
        filenames += zip(*[get_channel_file(t=t, channel=c) for c in channels])

    return filenames


class FakeScene(list):
    def __init__(self, source_files, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_files = source_files

def _load_channels_old(fns, cli):
    CHUNK_SIZE = 4096 # satpy uses this chunksize, so let's do the same
    REQUIRED_CHANNELS = [1,2,3]

    def set_projection_attribute_and_scale_coords(ds):
        gp = ds.goes_imager_projection

        globe = ccrs.Globe(ellipse='sphere', semimajor_axis=gp.semi_major_axis,
                           semiminor_axis=gp.semi_minor_axis)
        img_proj = ccrs.Geostationary(satellite_height=gp.perspective_point_height,
                                      central_longitude=gp.longitude_of_projection_origin,
                                      globe=globe)
        ds.attrs['crs'] = img_proj

        # coordinates are scaled by satellite height in image
        ds.coords['x'] = ds.x*gp.perspective_point_height
        ds.coords['y'] = ds.y*gp.perspective_point_height

        return ds

    def _load_file(fn):
        ds = xr.open_dataset(fn, chunks=dict(x=CHUNK_SIZE, y=CHUNK_SIZE))
        ds = set_projection_attribute_and_scale_coords(ds)

        da = ds.Rad
        da.attrs['crs'] = ds.crs
        da['channel'] = int(cli.parse_key(fn)['channel'])

        return da

    def fn_is_required(fn):
        return int(cli.parse_key(fn)['channel']) in REQUIRED_CHANNELS

    fns_required = list(filter(fn_is_required, fns))
    channel_da_arr = [_load_file(fn) for fn in fns_required]
    assert len(channel_da_arr) == len(REQUIRED_CHANNELS)

    # it would be tempting concat these into a single data array here, but we
    # can't because the different channels have different resolution
    da_scene = FakeScene(fns_required, channel_da_arr)

    return da_scene

def load_data_for_rgb(dataset_filenames, cli, use_old=True):
    das = []  # dataarrays

    for fns in dataset_filenames:
        if use_old:
            da_channels = _load_channels_old(fns=fns, cli=cli)

        das.append(da_channels)

    return das

def fetch_channels():
    def get_channel_file(t, channel):
        keys = cli.query(time=t, region='F', debug=False, channel=channel,
                         dt_max=dt_max)

        key = keys[0]


        ds = xr.open_dataset(fn)
        ds = set_projection_attribute_and_scale_coords(ds)

        da = ds.Rad
        da.attrs['crs'] = ds.crs
        da.attrs['channel'] = channel

        da.attrs['query_time'] = t
        da.attrs['aws_s3_key'] = key

        return da

    print("Fetching data")
    # fetch a channel set for each day
    channel_sets = [
        [get_channel_file(t=t, channel=c) for c in channels]
        for t in times
    ]

    return channel_sets

def generate_tile_triplets(scenes, tiling_bbox, tile_N, tile_size, output_dir,
                           N_triplets, max_workers=4):
    if len(scenes) < 2:
        raise Exception("Need at least two scenes")


    print("Generating tiles")

    arglist = []

    for n in range(N_triplets):
        # sample different datasets
        tn_target, tn_dist = random.sample(range(len(scenes)), 2)
        da_target_scene = scenes[tn_target]
        da_distant_scene = scenes[tn_dist]

        arglist.append(
            dict(
                neigh_dist_scaling=0.5,
                da_target_scene=da_target_scene,
                da_distant_scene=da_distant_scene,
                tile_size=tile_size,
                tiling_bbox=tiling_bbox,
                tile_N=tile_N,
                triplet_n=n,
                output_dir=output_dir,
            )
        )

    if max_workers != 1:
        parallel(triplet_fn, arglist, max_workers=4)
    else:
        for args in tqdm(arglist):
            triplet_fn(args)

TRIPLET_FN_FORMAT = "{:05d}_{}.png"

def triplet_fn(kws, *args, **kwargs):
    triplet_n = kws.pop('triplet_n')
    output_dir = kws.pop('output_dir')
    np.random.seed(int(time.time()+triplet_n))

    prefixes = "anchor neighbor distant".split(" ")

    output_files_exist = [
        os.path.exists(output_dir/TRIPLET_FN_FORMAT.format(triplet_n, p))
        for p in prefixes
    ]

    if all(output_files_exist):
        return

    tiles_and_imgs = tiler.triplet_generator(**kws)

    tiles, imgs = zip(*tiles_and_imgs)

    for (img, prefix) in zip(imgs, prefixes):
        fn_out = TRIPLET_FN_FORMAT.format(triplet_n, prefix)
        img.save(output_dir/fn_out, "PNG")

    da_target_scene = kws['da_target_scene']

    da_distant_scene = kws['da_distant_scene']

    meta = dict(
        target=dict(
            source_files=da_target_scene.source_files,
            anchor=tiles[0].serialize_props(),
            neighbor=tiles[1].serialize_props(),
        ),
        distant=dict(
            source_files=da_distant_scene.source_files,
            loc=tiles[2].serialize_props(),
        )
    )

    meta_fn = output_dir/"{:05d}_meta.yaml".format(triplet_n)
    with open(meta_fn, 'w') as fh:
        yaml.dump(meta, fh, default_flow_style=False)


class ProcessedTile(tiler.Tile):
    @classmethod
    def load(cls, meta_fn):
        tile_id = meta_fn.name.split('_')[0]
        meta = yaml.load(open(meta_fn))
        anchor_meta = meta['target']['anchor']
        tile = cls(
            lat0=anchor_meta['lat'],
            lon0=anchor_meta['lon'], 
            size=anchor_meta['size']
        )

        setattr(tile, 'id', tile_id)
        setattr(tile, 'source', meta['target']['aws_s3_key'])

        return tile

    def get_source(tile, channel_override):
        key_ch1 = tile.source
        key_ch = key_ch1.replace('M3C01', 'M3C{:02d}'.format(channel_override))

        da_channel = get_file(key_ch)

        return da_channel


if __name__ == "__main__":
    data_path = Path('../data/storage')
    lon = -60 # Barbardos is near -60W

    t_zenith = satdata.calc_nearest_zenith_time_at_loc(-60) 
    times = [t_zenith - datetime.timedelta(days=n) for n in range(3,13)]
