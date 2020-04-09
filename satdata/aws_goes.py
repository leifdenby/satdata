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
import re
from pathlib import Path
import warnings

import s3fs

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

    SENSOR_MODES = {
        3: "flex mode full disk every 15min",
        4: "continuous full disk every 5min",
        # mode 6 became default for full disk as of 2019-04-03,
        # see https://cimss.ssec.wisc.edu/goes/blog/archives/32657,
        # https://satelliteliaisonblog.com/2019/04/02/mode-6-replaces-mode-3/
        6: "flex mode full disk every 10min",
    }

    SENSOR_MODE_3_TO_4_TRANSITION_DATE = datetime.datetime(
        year=2019, month=4, day=2
    )

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

    URL = "https://registry.opendata.aws/noaa-goes/"

    KEY_REGEX = re.compile(".*/OR_ABI-L1b-Rad(?P<region>[\w\d]{1,2})-"
                          "M(?P<sensor_mode>\d)C(?P<channel>\d+)_"
                          "G16_s(?P<start_time>\d+)_"
                          "e(?P<end_time>\d+)_"
                          "c(?P<file_creation_time>\d+)"
                          "\.nc")

    def __init__(self, local_storage_dir='goes16', offline=False):
        self.offline = offline
        self.local_storage_dir = Path(local_storage_dir)

        if not offline:
            self.s3client = s3fs.S3FileSystem(anon=True)

    def _check_sensor_mode(self, sensor_mode, t):
        if sensor_mode == 6 and t <= self.SENSOR_MODE_3_TO_4_TRANSITION_DATE:
            warnings.warn("Sensor mode 6 wasn't available before {},"
                          " switching to mode 3".format(
                            str(self.SENSOR_MODE_3_TO_4_TRANSITION_DATE)
                          ))
            return 3
        elif sensor_mode == 3 and t > self.SENSOR_MODE_3_TO_4_TRANSITION_DATE:
            warnings.warn("Sensor mode 3 isn't available after {},"
                          " switching to mode 6".format(
                            str(self.SENSOR_MODE_3_TO_4_TRANSITION_DATE)
                          ))
            return 6
        else:
            return sensor_mode

    def make_prefix(self, t, product='Rad', sensor="ABI", region="C",
                    sensor_mode=6, channel=2):
        level = self.PRODUCT_LEVEL_MAP.get(product)

        sensor_mode = self._check_sensor_mode(sensor_mode, t)

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

        p = "{path_prefix}/{year}/{day_of_year:03d}/{hour:02d}/OR_{sensor}-{level}-{product}{region}-M{mode}C{channel:02d}".format(**dict(
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

    @classmethod
    def parse_key(cls, k, parse_times=False):
        match = cls.KEY_REGEX.match(k)
        if match:
            data = match.groupdict()

            for f in ['channel', 'sensor_mode']:
                data[f] = int(data[f])

            if parse_times:
                for (k, v) in data.items():
                    if k.endswith('_time'):
                        data[k] = cls.parse_timestamp(data[k])
            return data
        else:
            return None

    @staticmethod
    def parse_timestamp(s):
        """
        s20171671145342: is start of scan time
        4 digit year
        3 digit day of year
        2 digit hour
        2 digit minute
        2 digit second
        1 digit tenth of second
        """
        return datetime.datetime.strptime(s[:-1], "%Y%j%H%M%S")

    def query(self, time, dt_max=datetime.timedelta(hours=4), sensor="ABI",
              product="Rad", region="C", channel=2, sensor_mode=6, 
              include_in_glacier_storage=False, debug=False):

        t_max = time + dt_max
        t_min = time - dt_max

        # `<Product>/<Year>/<Day of Year>/<Hour>/<Filename>`
        # last part of prefix path is `hour`, so we list directories by hour
        # and filter later

        def build_paths():
            t = t_min
            while t <= t_max + datetime.timedelta(hours=24):
                # AWS stores files by hour, so we need to query a folder at a
                # time and then filter later
                prefix = self.make_prefix(
                    t=t,
                    product=product,
                    region=region,
                    channel=channel,
                    sensor_mode=sensor_mode
                )
                yield str(Path(prefix).parent)
                t += datetime.timedelta(hours=1)

        if not self.offline:
            keys = []
            for prefix in build_paths():
                if debug:
                    print("Quering prefix `{}`".format(prefix))
                keys += self.s3client.ls(
                    's3://{b}/{p}'.format(b=self.BUCKET_NAME, p=prefix)
                )
        else:
            if not self.local_storage_dir.exists():
                raise Exception("There's currently no directory `{}` for "
                                "for local storage and so offline queries "
                                "can't be done.".format(self.local_storage_dir))
            else:
                keys = []
                for prefix in build_paths():
                    fps = self.local_storage_dir.glob("{}*".format(prefix))
                    keys += [
                        str(fp.relative_to(self.local_storage_dir))
                        for fp in fps
                    ]

        def is_within_dt_max_tol(key):
            key_parts = self.parse_key(key, parse_times=True)
            t = key_parts['end_time']
            return t_min < t < t_max

        def _is_correct_product(key):
            key_parts = self.parse_key(key)
            return (key_parts['channel'] == channel and
                    key_parts['sensor_mode'] == sensor_mode and
                    key_parts['region'] == region
                    )

        keys = list(filter(is_within_dt_max_tol, keys))
        keys = list(filter(_is_correct_product, keys))

        return keys


    def download(self, key, overwrite=False, debug=False):
        if not type(key) == list:
            keys = [key,]
        else:
            keys = key

        if self.offline:
            # we'll just fake the download for convience by returning the paths
            # to the already downloaded files
            return [str(self.local_storage_dir.joinpath(k)) for k in keys]

        files = []

        for key in tqdm(keys):
            fn_out = os.path.join(self.local_storage_dir, key)

            dir = os.path.dirname(fn_out)
            if not os.path.exists(dir):
                os.makedirs(dir)

            if os.path.exists(fn_out) and not overwrite:
                if debug:
                    print("File `{}` already exists in `{}`".format(key, fn_out))
            else:
                if debug:
                    print("Downloading {} -> {}".format(key, fn_out))
                self.s3client.get(
                    's3://{p}'.format(p=key),
                    fn_out
                )

            files.append(fn_out)

        return files
