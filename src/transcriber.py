import html, logging
from datetime import datetime


class Transcriber:
    # TODO: This file should be its own package with subclasses honestly
    submission_attrs = [
        "id",
        "title",
        "body",
        "url",
        "epoch",
        "score",
        "subreddit",
        "author",
        "status",
        "shortlink",
    ]
    comment_attrs = [
        "id",
        "body",
        "epoch",
        "score",
        "subreddit",
        "author",
        "status",
        "shortlink",
    ]

    def __init__(self, item_contents, item_type=None, report_source_as=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.report_source_as = report_source_as
        self.valid = False
        self.origin = item_contents

        try:
            # TODO: Make decision tree to sniff at item type, implementation, etc.
            # TODO: Implement length limit validation on all fields
            if item_type == "submission":
                self.parse_submission()
            elif item_type == "comment":
                self.parse_comment()
        except Exception as e:
            self.logger.error(f"init failed: {e}")

    def optional_fetch(self, key):
        if key not in self.origin.keys():
            return ""
        return self.origin[key]

    def does_key_exist(self, key):
        if key in self.origin.keys():
            return True
        return False

    def interpret_status(self):
        if self.body == "[deleted]" or self.author == "[deleted]":
            self.status = 410
        elif self.body == "[removed]" or self.author == "[removed]":
            self.status = 403
        else:
            self.status = 200

        if self.does_key_exist("removed_by_category"):
            if self.origin["removed_by_category"] == "automod_filtered":
                self.status = 406
            elif self.origin["removed_by_category"] == "moderator":
                self.status = 403
            elif self.origin["removed_by_category"] == "reddit":
                self.status = 402  # haha social commentary :^)

    def parse_submission(self):
        self.type = "submission"

        self.id = int(self.origin["id"], 36)
        self.title = html.unescape(self.optional_fetch("title"))
        self.body = html.unescape(self.optional_fetch("selftext"))

        if (
            self.optional_fetch("url")
            == "https://www.reddit.com" + self.origin["permalink"]
        ):
            self.url = ""
        else:
            self.url = self.optional_fetch("url")

        self.epoch = int(self.origin["created_utc"])
        self.score = int(self.origin["score"])
        self.subreddit = self.origin["subreddit"]
        self.author = self.optional_fetch("author")
        self.shortlink = self.origin["id"]

        self.interpret_status()

        self.valid = True

    def parse_comment(self):
        self.type = "comment"

        self.id = int(self.origin["id"], 36)
        self.body = html.unescape(self.optional_fetch("body"))
        self.epoch = int(self.origin["created_utc"])
        self.score = int(self.origin["score"])
        self.subreddit = self.origin["subreddit"]
        self.author = self.optional_fetch("author")
        self.shortlink = self.origin["link_id"][3:]

        self.interpret_status()

        self.valid = True

    def get_dict(self):
        db_format_result = {}
        if self.type == "submission":
            for source in self.submission_attrs:
                db_format_result[source] = getattr(self, source)
        elif self.type == "comment":
            for source in self.comment_attrs:
                db_format_result[source] = getattr(self, source)
        return db_format_result
