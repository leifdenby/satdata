# satdata

![Python
application](https://github.com/leifdenby/satdata/workflows/Python%20application/badge.svg)

Simplified access to GOES-16 data from python or the command line

## Installation

For the moment you will need to install directly from github:

    $> pip install git+https://github.com/leifdenby/satdata?egg=satdata

or clone the repository and use pip to install the local copy

    $> git clone https://github.com/leifdenby/satdata
    $> pip install satdata/


## Usage

You can use the command line interface to query and download files:

    $> python -m satdata.cli --help
```
usage: cli.py [-h] [--dt_max DT_MAX] [--region {F,C,M1,M2}]
              [--channel {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}]
              [--fetch-files] [--debug]
              time

positional arguments:
  time                  query for files around this time, an ISO8601 formatted
                        time,for example 2020-01-20T16:20Z

optional arguments:
  -h, --help            show this help message and exit
  --dt_max DT_MAX       query-window around `time`, an ISO8601 formatted
                        duration for example P1D for one day or PT15M for 15
                        minutes
  --region {F,C,M1,M2}  F: full disk, C: continential US, M1: mesoscale region
                        1 (west), M2: mesoscale region 2 (east)
  --channel {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}
                        1: Blue, 2: Red, 3: Veggie, 4: Cirrus, 5: Snow/Ice, 6:
                        Cloud Particle Size, 7: Shortwave Window, 8: Upper-
                        Level tropispheric water vapour, 9: Mid-level
                        tropospheric water vapour, 10: Lower-level water
                        vapour, 11: Cloud-top phase, 12: Ozone, 13: 'Clean' IR
                        longwave window, 14: 'Dirty' IR longwave window, 15:
                        'Dirty' longwave window, 16: 'CO2' longwave infrared
  --fetch-files         fetch all files that match query (default is just to
                        print files that match query
  --debug               show debug info
```


Or use the module directly from python:

```python
import datetime
import satdata


t = datetime.datetime(day=16, month=1, year=2020, hour=16, minute=0)
dt_max = datetime.timedelta(minutes=20)

cli = satdata.Goes16AWS()
keys = cli.query(time=t, region='F', debug=True, dt_max=dt_max)
fn = cli.download(keys[0])[0]
```
