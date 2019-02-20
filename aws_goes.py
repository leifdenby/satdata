"""
ABI-L1b-RadF-M3C02 is delineated by hyphen '-':
jBI: is ABI Sensor
L1b: is processing level, L1b data or L2
Rad: is radiances. Other products include CMIP (Cloud and Moisture Imagery products) and MCMIP (multichannel CMIP).
F: is full disk (normally every 15 minutes), C is continental U.S. (normally every 5 minutes), M1 and M2 is Mesoscale region 1 and region 2 (usually every minute each)
M3: is mode 3 (scan operation), M4 is mode 4 (only full disk scans every five minutes - no mesoscale or CONUS)
C02: is channel or band 02, There will be sixteen bands, 01-16
"""
import datetime
import os
import subprocess

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from tqdm import tqdm


class Goes16AWS:
    BUCKET_NAME = 'noaa-goes16'
    BUCKET_REGION = 'us-east-1'

    PRODUCT_LEVEL_MAP = dict(
        CMIP="L2",
        Rad="L1b",
    )

    REGIONS = dict(
        F="full disk",
        C="continential US",
        M1="mesoscale region 1 (west)",
        M2="mesoscale region 2 (east)",
    )

    SENSOR_MODES = {3: "scan operation", 4: "full disk every 5min"}

    CHANNELS = {
        1:"Blue",
        2:"Red",
        3:"Veggie",
        4:"Cirrus",
        5:"Snow/Ice",
        6:"Cloud Particle Size",
        7:"Shortwave Window",
        8:"Upper-Level tropispheric water vapour",
        9:"Mid-level tropospheric water vapour",
        10:"Lower-level water vapour",
        11:"Cloud-top phase",
        12:"Ozone",
        13:"'Clean' IR longwave window",
        14:"'Dirty' IR longwave window",
        15:"'Dirty' longwave window",
        16:"'CO2' longwave infrared"
    }

    def __init__(self):
        # to access a public bucket we must indicate to boto not to sign requests
        # (https://stackoverflow.com/a/34866092)
        self.boto_config = Config(signature_version=UNSIGNED)
        self.s3client = boto3.client('s3',
            region_name=self.BUCKET_REGION,
            config=self.boto_config
        )

    def make_prefix(self, t, product='Rad', sensor="ABI", region="C",
                    sensor_mode=3, channel=2):
        level = self.PRODUCT_LEVEL_MAP.get(product)

        if level is None:
            raise NotImplementedError("Level for {} unknown".format(product))

        if not region in self.REGIONS.keys():
            raise Exception("`region` should be one of:\n{}".format(
                ", ".join([
                    "\t{}: {}\n".format(k, v) for (k,v) in self.REGIONS.items()
                ])
            ))

        # for some reason the mesoscale regions use the same folder prefix...
        region_ = region
        if region in ['M1', 'M2']:
            region_ = 'M'

        path_prefix = "{sensor}-{level}-{product}{region}".format(
            sensor=sensor, product=product, region=region_, level=level,
        )

        p = "{path_prefix}/{year}/{day_of_year:03d}/{hour}/OR_{sensor}-{level}-{product}{region}-M{mode}C{channel:02d}".format(**dict(
               path_prefix=path_prefix,
               product=product,
               day_of_year=t.timetuple().tm_yday,
               year=t.year,
               hour=t.hour,
               sensor=sensor,
               mode=sensor_mode,
               channel=channel,
               level=level,
               region=region,
        ))
        return p

    def query(self, time, sensor="ABI", product="Rad", region="C", channel=2,
              sensor_mode=3, include_in_glacier_storage=False, debug=False):
        prefix = self.make_prefix(
            t=time,
            product=product,
            region=region,
            channel=channel,
            sensor_mode=sensor_mode
        )

        if debug:
            print("Quering prefix `{}`".format(prefix))

        req = self.s3client.list_objects(
            Bucket=self.BUCKET_NAME, Prefix=prefix
        )

        if not 'Contents' in req:
            return []

        objs = req['Contents']

        if not include_in_glacier_storage:
            objs = filter(lambda o: o['StorageClass'] != "GLACIER", objs)

        return list(map(lambda o: o['Key'], objs))


    def download(self, key, output_dir='goes16', overwrite=False, debug=False):

        if not type(key) == list:
            keys = [key,]
        else:
            keys = key

        files = []

        for key in tqdm(keys):
            fn_out = os.path.join(output_dir, key)

            dir = os.path.dirname(fn_out)
            if not os.path.exists(dir):
                os.makedirs(dir)

            if os.path.exists(fn_out) and not overwrite:
                if debug:
                    print("File `{}` already exists in `{}`".format(key, fn_out))
            else:
                self.s3client.download_file(
                    self.BUCKET_NAME, key, fn_out
                )

            files.append(fn_out)

        return files


def execute(cmd):
    # https://stackoverflow.com/a/4417735
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()

    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
