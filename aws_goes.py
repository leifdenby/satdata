"""
ABI-L1b-RadF-M3C02 is delineated by hyphen '-':
ABI: is ABI Sensor
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

        path_prefix = "{sensor}-{level}-{product}{region}".format(
            sensor=sensor, product=product, region=region, level=level,
        )

        # return path_prefix


        p = "{path_prefix}/{year}/{day_of_year}/{hour}/OR_{sensor}-{level}-{product}{region}-M{mode}C{channel:02d}".format(**dict(
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
              sensor_mode=3, include_in_glacier_storage=False):
        prefix = self.make_prefix(
            t=time,
            product=product,
            region=region,
            channel=channel,
            sensor_mode=sensor_mode
        )

        objs = self.s3client.list_objects(
            Bucket=self.BUCKET_NAME, Prefix=prefix
        )['Contents']

        if not include_in_glacier_storage:
            objs = filter(lambda o: o['StorageClass'] != "GLACIER", objs)

        return map(lambda o: o['Key'], objs)


    def download_file(self, key):
        fn_out = key.split('/')[-1]
        cmd = "aws s3 cp s3://noaa-goes16/{key} {filename} --no-sign-request".format(key=key, filename=fn_out)

        proc = subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE,
                stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise Exception(err)

        print(out)
