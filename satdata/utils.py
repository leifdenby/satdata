import datetime


def calc_zenith_time_offset_at_loc(lon):
    T_period = 24 * 60.0 * 60.0  # [s]

    dTddeg = T_period / 360.0

    dt = lon * dTddeg

    return datetime.timedelta(seconds=-dt)


def calc_nearest_zenith_time_at_loc(lon, t_ref=None):
    if t_ref is None:
        t_ref = datetime.datetime.today()

    dt_till_midday_today = datetime.timedelta(
        minutes=t_ref.minute,
        hours=t_ref.hour - 12,
        seconds=t_ref.second,
        microseconds=t_ref.microsecond,
    )

    dt_lon_offset = calc_zenith_time_offset_at_loc(lon=lon)
    dt_till_nearest_zenith = -dt_till_midday_today + dt_lon_offset

    if dt_till_nearest_zenith.total_seconds() / (60 * 60) < -12:
        dt_till_nearest_zenith += datetime.timedelta(hours=24)

    if dt_till_nearest_zenith.total_seconds() / (60 * 60) > 12:
        dt_till_nearest_zenith -= datetime.timedelta(hours=24)

    t_zenith_local = t_ref + dt_till_nearest_zenith

    return t_zenith_local
