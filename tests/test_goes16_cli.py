from pathlib import Path
import datetime

import satdata


def test_fetch_one_channel():
    lon_zenith = -45.0
    t0 = satdata.calc_nearest_zenith_time_at_loc(lon_zenith)
    t = t0 - datetime.timedelta(days=3)
    dt_max = datetime.timedelta(minutes=20)

    cli = satdata.Goes16AWS()

    keys = cli.query(time=t, region="F", debug=True, dt_max=dt_max, channel=1)

    fn = cli.download(keys[0])[0]

    assert Path(fn).exists()


def test_fetch_one_channel_multi_day():
    N_HOURS = 2
    lon_zenith = -45.0

    dt_max = datetime.timedelta(hours=N_HOURS)
    t0 = satdata.calc_nearest_zenith_time_at_loc(lon_zenith)
    t = t0 - datetime.timedelta(days=8)

    cli = satdata.Goes16AWS()

    keys = cli.query(time=t, dt_max=dt_max, region="F", debug=True, channel=1)

    # imagery should be available at least every 15 mins in the F region
    print("\n".join(keys))
    print(len(keys), (1 + 2 * N_HOURS) * 60 / 15)
    assert len(keys) > (1 + 2 * N_HOURS) * 60 / 15


def test_parse_L2_key():
    key = "noaa-goes16/ABI-L2-TPWC/2020/037/00/OR_ABI-L2-TPWC-M6_G16_s20200370001071_e20200370003443_c20200370005459.nc"
    c = satdata.Goes16AWS()
    parsed_data = c.parse_key(key)

    data = dict(
        level="L2",
        product="TPW",
        region="C",
        sensor_mode=6,
    )

    for k, v in data.items():
        assert parsed_data[k] == v


def test_parse_L1_key():
    key = "noaa-goes16/ABI-L1b-RadC/2020/037/00/OR_ABI-L1b-RadC-M6C02_G16_s20200370051071_e20200370053443_c20200370053496.nc"
    c = satdata.Goes16AWS()
    parsed_data = c.parse_key(key)

    data = dict(
        level="L1b",
        product="Rad",
        channel=2,
        region="C",
        sensor_mode=6,
    )

    for k, v in data.items():
        assert parsed_data[k] == v
