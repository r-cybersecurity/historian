from src.pushshift import Pushshift
import argparse, logging, signal, sys

tolerant_stop = True


def graceful_stop(signum, frame):
    global tolerant_stop
    if not tolerant_stop:
        logging.critical("Impatient - killed threads")
        sys.exit()
    tolerant_stop = False
    logging.warning("Beginning graceful stop")
    pushshift.stop_thread()
    logging.warning("System stopped gracefully")


parser = argparse.ArgumentParser(
    description=(
        "Consumes Reddit data from Pushshift, then normalizes and stores it. "
        "By default, fetches and prints all data from certain defined subreddits. "
        "Optionally, stores data or pulls only the most recent data. "
    )
)
parser.add_argument(
    "--cron", help="Fetches ~4d of posts, not everything", action="store_true",
)
parser.add_argument(
    "--type",
    help="Changes type of data to fetch (default: submission) (can use 'both')",
    choices={"comment", "submission", "both"},
    default="submission",
)
parser.add_argument(
    "--subreddits", type=str, nargs="+", help="Subreddits to fetch data for",
)

parser.add_argument(
    "-d", "--debug", help="Output a metric shitton of runtime data", action="store_true"
)
parser.add_argument(
    "-v",
    "--verbose",
    help="Output a reasonable amount of runtime data",
    action="store_true",
)

args = parser.parse_args()
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
elif args.verbose:
    logging.basicConfig(level=logging.INFO)

pushshift = Pushshift()
pushshift.start_thread()

signal.signal(signal.SIGINT, graceful_stop)

for subreddit in args.subreddits:
    pushshift.pull_subreddit(subreddit, args.type, args.cron)

pushshift.stop_thread()
