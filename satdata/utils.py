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

    t_zenith_local = t_ref - dt_till_midday_today + dt_lon_offset

    if t_zenith_local > t_ref:
        t_zenith_local -= datetime.timedelta(hours=24)

    return t_zenith_local
