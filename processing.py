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

def fetch_channels(times, dt_max, cli, data_path, channels=[1,2,3]):
    def get_channel_file(t, channel):
        keys = cli.query(time=t, region='F', debug=False, channel=channel,
                         dt_max=dt_max)

        key = keys[0]

        fn = cli.download(key, output_dir=data_path/"sources"/"goes16")[0]

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

def generate_tile_triplets(channel_sets, tiling_bbox, tile_N, tile_size, data_path,
                           N_triplets, max_workers=4):
    if len(channel_sets) < 2:
        raise Exception("Need at least two channel_sets")


    print("Generating tiles")

    tile_path = data_path/"tiles"/"goes16"/"N{}_s{}".format(tile_N, tile_size)
    tile_path.mkdir(exist_ok=True, parents=True)

    arglist = []

    for n in range(N_triplets):
        # sample different datasets
        tn_target, tn_dist = random.sample(range(len(channel_sets)), 2)
        target_channels = channel_sets[tn_target]
        distant_channels = channel_sets[tn_dist]

        arglist.append(
            dict(
                neigh_dist_scaling=0.5,
                target_channels=target_channels,
                distant_channels=distant_channels,
                tile_size=tile_size,
                tiling_bbox=tiling_bbox,
                tile_N=tile_N,
                triplet_n=n,
                tile_path=tile_path,
            )
        )

    if max_workers != 1:
        parallel(triplet_fn, arglist, max_workers=4)
    else:
        for args in arglist:
            triplet_fn(args)

TRIPLET_FN_FORMAT = "{:05d}_{}.png"

def triplet_fn(kws, *args, **kwargs):
    triplet_n = kws.pop('triplet_n')
    tile_path = kws.pop('tile_path')
    np.random.seed(int(time.time()+triplet_n))

    prefixes = "anchor neighbor distant".split(" ")

    output_files_exist = [
        os.path.exists(tile_path/TRIPLET_FN_FORMAT.format(triplet_n, p))
        for p in prefixes
    ]

    if all(output_files_exist):
        return

    tiles_and_imgs = tiler.triplet_generator(**kws)

    tiles, imgs = zip(*tiles_and_imgs)

    for (img, prefix) in zip(imgs, prefixes):
        fn_out = TRIPLET_FN_FORMAT.format(triplet_n, prefix)
        img.save(tile_path/fn_out, "PNG")

    target_channels = kws['target_channels']
    da_target_ch0 = target_channels[0][0]

    distant_channels = kws['distant_channels']
    da_distant_ch0 = distant_channels[0][2]

    meta = dict(
        target=dict(
            aws_s3_key=da_target_ch0.aws_s3_key,
            anchor=tiles[0].serialize_props(),
            neighbor=tiles[1].serialize_props(),
        ),
        distant=dict(
            aws_s3_key=da_distant_ch0.aws_s3_key,
            loc=tiles[2].serialize_props(),
        )
    )

    meta_fn = tile_path/"{:05d}_meta.yaml".format(triplet_n)
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
