from pathlib import Path
import datetime

import satdata

def test_fetch_one_channel():
    lon_zenith = -45.
    t0 = satdata.calc_nearest_zenith_time_at_loc(lon_zenith)
    t = t0 - datetime.timedelta(days=3)
    dt_max = datetime.timedelta(minutes=20)

    cli = satdata.Goes16AWS()

    keys = cli.query(time=t, region='F', debug=True, dt_max=dt_max)

    fn = cli.download(keys[0])[0]

    assert Path(fn).exists()

def test_fetch_one_channel_multi_day():
    N_HOURS = 2
    lon_zenith = -45.

    dt_max = datetime.timedelta(hours=N_HOURS)
    t0 = satdata.calc_nearest_zenith_time_at_loc(lon_zenith)
    t = t0 - datetime.timedelta(days=8)

    cli = satdata.Goes16AWS()

    keys = cli.query(time=t, dt_max=dt_max, region='F', debug=True)

    # imagery should be available at least every 15 mins in the F region
    print("\n".join(keys))
    print(len(keys), (1+2*N_HOURS)*60/15)
    assert len(keys) > (1+2*N_HOURS)*60/15
