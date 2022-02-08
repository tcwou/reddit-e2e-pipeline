import argparse

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--end_date", nargs=3, type=int, default=None)
    parser.add_argument("--start_date", nargs=3, type=int, default=None)
    parser.add_argument("--start_date_delta", type=int, default=1)
    parser.add_argument("--day_offset", type=int, default=0)
    args = parser.parse_args()

    if args.end_date:
        try:
            args.end_date = tuple(args.end_date)
        except:
            args.end_date = None

    if args.start_date:
        try:
            args.start_date = tuple(args.start_date)
        except:
            args.start_date = None

    return args