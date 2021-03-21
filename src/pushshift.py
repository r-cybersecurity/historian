import logging, requests, time
from datetime import datetime

from config.config import *
from src.transcriber import Transcriber
from unsafemysql import UnsafeMySQLWriter, UnsafeMySQLItem


class Pushshift:
    db_submission_insert_update = (
        "INSERT INTO `reddit-submissions` "
        "(id, title, body, url, epoch, score, subreddit, author, status, shortlink) "
        "VALUES(%(id)s, %(title)s, %(body)s, %(url)s, %(epoch)s, %(score)s, %(subreddit)s, %(author)s, %(status)s, %(shortlink)s) "
        "ON DUPLICATE KEY UPDATE "
        "status = %(status)s, score = %(score)s"
    )

    db_comment_insert_update = (
        "INSERT INTO `reddit-comments` "
        "(id, body, epoch, score, subreddit, author, status, shortlink) "
        "VALUES(%(id)s, %(body)s, %(epoch)s, %(score)s, %(subreddit)s, %(author)s, %(status)s,  %(shortlink)s) "
        "ON DUPLICATE KEY UPDATE "
        "status = %(status)s, score = %(score)s"
    )

    query_subreddit_url = "https://api.pushshift.io/reddit/{}/search?limit=100&sort=desc&subreddit={}&before="
    our_headers = {"User-Agent": "r-cybersecurity/historian"}

    def __init__(self):
        self.start_epoch = int(datetime.utcnow().timestamp())
        self.logger = logging.getLogger(self.__class__.__name__)
        self.unsafedb = UnsafeMySQLWriter(
            db_config_host,
            db_config_port,
            db_config_user,
            db_config_passwd,
            db_config_database,
        )

    def common_pushshift_setup(self):
        self.logger.info("setting up to roll through pushshift API")
        self.item_count = 0
        self.retry_count = 0
        self.additional_backoff = 1

    def pull_subreddit(self, subreddit, item_type, update_flag):
        self.common_pushshift_setup()
        previous_epoch = self.start_epoch
        item_count = 0

        self.logger.info(f"ingesting {item_type}s from {subreddit}")
        while True:
            new_url = self.query_subreddit_url.format(item_type, subreddit) + str(
                previous_epoch
            )

            try:
                fetched_data = requests.get(new_url, headers=self.our_headers)
            except Exception as e:
                self.common_pushshift_backoff("api", e)
                if self.retry_count >= 13:
                    break
                continue

            try:
                json_data = fetched_data.json()
            except Exception as e:
                self.common_pushshift_backoff("json", e)
                if self.retry_count >= 13:
                    break
                continue

            if "data" not in json_data:
                self.common_pushshift_backoff("json", "data not in json")
                if self.retry_count >= 13:
                    break
                continue

            items = json_data["data"]
            self.retry_count = 0
            self.additional_backoff = 1

            if len(items) == 0:
                self.logger.info(
                    f"pushshift API returned no more {item_type}s for {subreddit}"
                )
                break

            for item in items:
                previous_epoch = item["created_utc"] - 1
                item_count += 1

                try:
                    transcription = Transcriber(
                        item, item_type
                    )
                except Exception as e:
                    self.logger.warning(f"exception caught when transcribing: {e}")
                    continue

                if transcription.valid:
                    if transcription.type == "submission":
                        item_to_save = UnsafeMySQLItem(
                            self.db_submission_insert_update,
                            transcription.get_dict(),
                            new_url,
                        )
                    elif transcription.type == "comment":
                        item_to_save = UnsafeMySQLItem(
                            self.db_comment_insert_update,
                            transcription.get_dict(),
                            new_url,
                        )
                    else:
                        self.logger.critical(
                            f"item type not implemented: {transcription.type}"
                        )
                    self.unsafedb.put_data(item_to_save)
                else:
                    self.logger.warning(f"an item was not valid from: {item['from']}")

            tempstamp = datetime.fromtimestamp(previous_epoch).strftime("%Y-%m-%d")
            self.logger.debug(
                f"retrieved {item_count} {item_type}s through {tempstamp}"
            )

            # lazily check for errors
            bad_item = self.unsafedb.get_failure()
            if bad_item is not None:
                self.logger.critical(
                    f"error from UnsafeMySQL - source: {bad_item.notes} ; problem: {bad_item.error}"
                )

            if update_flag and self.start_epoch - previous_epoch > 345600:
                self.logger.info(f"stopping pull for {subreddit} due to update flag")
                break

        self.logger.debug("quit pull_subreddit() loop")

    def common_pushshift_backoff(self, src, err):
        self.additional_backoff = self.additional_backoff * 2
        self.logger.info(f"backing off due to {src} error: {err}")
        self.retry_count = self.retry_count + 1
        time.sleep(self.additional_backoff)
