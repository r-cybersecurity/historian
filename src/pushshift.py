import logging, queue, requests, threading, time
from datetime import datetime
import mysql.connector
from mysql.connector import Error

from src.db import SolitudeDB
from src.transcriber import Transcriber


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
        self.write_queue = queue.Queue(maxsize=6000)
        self.kill_threads = False

    def start_db_thread(self):
        self.logger.info("starting data processor")
        self.active_data_processor = threading.Thread(target=self.write_thread_loop)
        self.active_data_processor.start()

    def stop_db_thread(self):
        self.logger.info("setting kill bit, waiting for graceful thread stop")
        self.kill_threads = True
        self.active_data_processor.join()
        self.logger.info("threads have closed")

    def write_thread_loop(self):
        self.write_db = SolitudeDB()
        while True:
            try:
                self.write_thread()
                if self.kill_threads and self.write_queue.empty():
                    try:  # we pretty much take it or leave it here
                        self.write_db.commit_write()
                    except Exception:
                        pass
                    self.write_db.disconnect()
                    break
            except Exception as e:
                self.logger.error(f"write_thread threw error: {e}")
                time.sleep(5)

    def write_thread(self):
        if self.write_queue.empty():
            time.sleep(1)
            return

        item = self.write_queue.get()

        try:
            transcription = Transcriber(item["contents"], item["type"], item["from"])
        except Exception as e:
            self.logger.warning(f"exception caught when transcribing: {e}")
            return

        if transcription.valid:
            item_db_dict = transcription.get_dict()
            if transcription.type == "submission":
                self.write_db.write(
                    self.db_submission_insert_update, item_db_dict,
                )
            elif transcription.type == "comment":
                self.write_db.write(
                    self.db_comment_insert_update, item_db_dict,
                )
            else:
                self.logger.critical(f"item type not implemented: {transcription.type}")
        else:
            self.logger.warning(f"an item was not valid from: {item['from']}")

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

                self.write_queue.put(
                    {"from": new_url, "type": item_type, "contents": item}
                )

            tempstamp = datetime.fromtimestamp(previous_epoch).strftime("%Y-%m-%d")
            self.logger.debug(
                f"retrieved {item_count} {item_type}s through {tempstamp}"
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
