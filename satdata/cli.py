import argparse
import isodate
import datetime

from . import aws_goes


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "time",
        type=isodate.parse_datetime,
        help=(
            "query for files around this time, an ISO8601 formatted time,"
            "for example 2020-01-20T16:20Z"
        ),
    )
    argparser.add_argument(
        "--dt_max",
        type=isodate.parse_date,
        help=(
            "query-window around `time`, an ISO8601 formatted duration"
            " for example P1D for one day or PT15M for 15 minutes"
        ),
        default=datetime.timedelta(minutes=20),
    )
    argparser.add_argument(
        "--region",
        choices=aws_goes.Goes16AWS.REGIONS.keys(),
        type=str,
        help=", ".join(
            ["{}: {}".format(k, v) for (k, v) in aws_goes.Goes16AWS.REGIONS.items()]
        ),
    )
    argparser.add_argument(
        "--channel",
        choices=aws_goes.Goes16AWS.CHANNELS.keys(),
        type=int,
        help=", ".join(
            ["{}: {}".format(k, v) for (k, v) in aws_goes.Goes16AWS.CHANNELS.items()]
        ),
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
    args = argparser.parse_args()

    cli = aws_goes.Goes16AWS()
    keys = cli.query(
        time=args.time,
        dt_max=args.dt_max,
        region=args.region,
        channel=args.channel,
        debug=args.debug,
    )

    if not args.fetch_files:
        print("Available files ({}):".format(len(keys)))
        print("\t{}".format("\n\t".join(keys)))
    else:
        cli.download(keys)


if __name__ == "__main__":
    main()
