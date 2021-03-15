from src.pushshift import Pushshift
import argparse, logging, os, signal, sys, yaml

tolerant_stop = True


def graceful_stop(signum, frame):
    global tolerant_stop
    if not tolerant_stop:
        logging.critical("Impatient - killed threads")
        sys.exit()
    tolerant_stop = False
    logging.warning("Beginning graceful stop")
    pushshift.stop_db_thread()
    logging.warning("System stopped gracefully")


parser = argparse.ArgumentParser(
    description=(
        "Consumes Reddit data from Pushshift, then normalizes and stores it. "
        "By default, fetches and prints all data from certain defined subreddits. "
        "Optionally, stores data or pulls only the most recent data. "
    )
)
parser.add_argument(
    "-c", "--cron", help="Fetches ~4d of posts, not everything", action="store_true",
)
parser.add_argument(
    "-t",
    "--type",
    help="Changes type of data to fetch (default: submission) (can use 'both')",
    choices={"comment", "submission", "both"},
    default="submission",
)
parser.add_argument(
    "-s",
    "--subreddits",
    type=str,
    nargs="+",
    help="List of subreddits to fetch data for; can also be a YAML file",
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
    log_level = logging.DEBUG
elif args.verbose:
    log_level = logging.INFO
else:
    log_level = logging.WARNING

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=log_level
)

subreddits = []
if os.path.isfile(args.subreddits[0]):
    with open(args.subreddits[0], "r") as config_file:
        yaml_config = yaml.safe_load(config_file)
        for classification, subreddits_from_classification in yaml_config.items():
            for subreddit_from_classification in subreddits_from_classification:
                subreddits.append(subreddit_from_classification)
else:
    subreddits = args.subreddits

pushshift = Pushshift()
pushshift.start_db_thread()

signal.signal(signal.SIGINT, graceful_stop)

types_to_fetch = []
if args.type == "both":
    types_to_fetch.append("submission")
    types_to_fetch.append("comment")
else:
    types_to_fetch.append(args.type)

for type_to_fetch in types_to_fetch:
    for subreddit in subreddits:
        pushshift.pull_subreddit(subreddit, type_to_fetch, args.cron)

pushshift.stop_db_thread()
