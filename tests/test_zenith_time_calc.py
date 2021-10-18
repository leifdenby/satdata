from satdata import utils
import datetime


def test_zenith_calc():
    t0 = datetime.datetime(year=2020, month=2, day=2, hour=12, minute=0)

    # move all the way around Earth
    for lon in range(-360, 360, 10):
        dt_zenith = utils.calc_zenith_time_offset_at_loc(lon=lon)
        t_zenith = t0 + dt_zenith

        # check that we've finding the correct zenith times from just 15min
        # before and after
        for n_minutes in [-15, 0, 15]:
            t_ref = t_zenith + datetime.timedelta(minutes=n_minutes)

            t_zenith_calc = utils.calc_nearest_zenith_time_at_loc(lon=lon, t_ref=t_ref)

            assert abs(t_zenith - t_zenith_calc) < datetime.timedelta(minutes=20)
