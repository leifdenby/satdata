import argparse
import datetime

import isodate

from . import aws_goes


def main(args=None):
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "time",
        type=isodate.parse_datetime,
        help=(
            "query for files around this time, an ISO8601 formatted time,"
            "for example 2020-01-20T16:20Z"
        ),
    )
    time_group = argparser.add_mutually_exclusive_group()
    time_group.add_argument(
        "--dt_max",
        type=isodate.parse_duration,
        help=(
            "query-window around `time`, an ISO8601 formatted duration"
            " for example P1D for one day or PT15M for 15 minutes"
        ),
        default=datetime.timedelta(hours=1),
    )
    time_group.add_argument(
        "--nearest-in-time",
        help="only return dataset closest in time",
        action="store_true",
        default=False,
    )
    argparser.add_argument(
        "--region",
        choices=aws_goes.Goes16AWS.REGIONS.keys(),
        type=str,
        help=", ".join(
            ["{}: {}".format(k, v) for (k, v) in aws_goes.Goes16AWS.REGIONS.items()]
        ),
        default="F",
    )
    command_group = argparser.add_mutually_exclusive_group()
    command_group.add_argument(
        "--channel",
        choices=aws_goes.Goes16AWS.CHANNELS.keys(),
        type=int,
        help="Radiance channel, options: "
        + ", ".join(
            ["{}: {}".format(k, v) for (k, v) in aws_goes.Goes16AWS.CHANNELS.items()]
        ),
    )
    command_group.add_argument(
        "--product",
        choices=aws_goes.Goes16AWS.PRODUCTS.keys(),
        type=str,
        help="Derived products, options: "
        + ", ".join(
            ["{}: {}".format(k, v) for (k, v) in aws_goes.Goes16AWS.PRODUCTS.items()]
        ),
        default="Rad",
    )

    argparser.add_argument(
        "--fetch-files",
        default=False,
        action="store_true",
        help=(
            "fetch all files that match query (default is just to print"
            " files that match query"
        ),
    )
    argparser.add_argument(
        "--debug", default=False, action="store_true", help=("show debug info")
    )
    args = argparser.parse_args(args)

    cli = aws_goes.Goes16AWS()
    keys = cli.query(
        time=args.time,
        dt_max=args.dt_max,
        region=args.region,
        channel=args.channel,
        product=args.product,
        debug=args.debug,
    )

    if args.nearest_in_time:

        def get_time_offset_for_key(k):
            t = aws_goes.Goes16AWS.parse_key(k, parse_times=True)["start_time"]
            return abs(args.time - t)

        keys = sorted(keys, key=get_time_offset_for_key)[:1]

    if not args.fetch_files:
        print("Available files ({}):".format(len(keys)))
        print("\t{}".format("\n\t".join(keys)))
    else:
        cli.download(keys)


if __name__ == "__main__":
    main()
