import datetime

def calc_zenith_time_offset_at_loc(lon):
    T_period = 24*60.*60.  # [s]

    dTddeg = T_period/360.

    dt = lon*dTddeg

    return datetime.timedelta(seconds=-dt)


def calc_nearest_zenith_time_at_loc(lon):
    t_now = datetime.datetime.today()

    dt_till_midday_today = datetime.timedelta(
        minutes=t_now.minute,
        hours=t_now.hour-12,
        seconds=t_now.second,
        microseconds=t_now.microsecond
    )

    dt_lon_offset = calc_zenith_time_offset_at_loc(lon=lon)

    t_zenith_local = t_now - dt_till_midday_today + dt_lon_offset

    if t_zenith_local > t_now:
        t_zenith_local -= datetime.timedelta(hours=24)

    return t_zenith_local
