from pathlib import Path
import datetime

import satdata

def test_fetch_one_channel():
    lon_zenith = -45.
    t0 = satdata.calc_nearest_zenith_time_at_loc(lon_zenith)
    t = t0 - datetime.timedelta(days=3)

    cli = satdata.Goes16AWS()

    print(t)
    keys = cli.query(time=t, region='F', debug=True)

    fn = cli.download(keys[0])[0]

    assert Path(fn).exists()
