from config.config import *
import mysql.connector
from mysql.connector import Error
import json, logging, time


class SolitudeDB:
    # I'm lazy and modifing a wrapper I made for MySQL for a system that needed:
    #  - thread safety
    #  - fault tolerance
    # In addition, this version includes the capability to hold cursors open.
    def __init__(self):
        self.max_retries = 4
        self.commit_at = 100
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_connection(self):
        self.connection = None
        self.cursor = None
        self.commit_ctr = 0

        try:
            self.connection = mysql.connector.connect(
                host=db_config_host,
                port=db_config_port,
                user=db_config_user,
                passwd=db_config_passwd,
                database=db_config_database,
            )
        except Exception as e:
            self.logger.error(f"create_connection threw error: {e}")

    def soft_restart_connection(self):
        try:
            self.connection.close()
        except Exception:
            self.logger.debug("soft_restart_connection continuing")
        self.create_connection()

    def disconnect(self):
        try:
            self.connection.close()
        except Exception:
            pass

    def read(self, sql, query):
        retry_count = 1

        while True:
            if self.connection == None:
                self.create_connection()

            try:
                cursor = self.connection.cursor(dictionary=True)
                cursor.execute(sql, query)
                data = cursor.fetchall()
                cursor.close()
                return [True, data]

            except Exception as e:
                self.logger.warning(f"read threw: {e}")
                retry_count = retry_count + 1

                if retry_count >= self.max_retries:
                    break

                time.sleep(5 * retry_count)
                self.soft_restart_connection()

        self.logger.error(f"exceeded retries, read failed with: {e}")
        return [False, None]

    def write(self, sql, data):
        retry_count = 1

        while True:
            if self.connection == None:
                self.create_connection()

            try:
                if self.commit_ctr == 0 or self.cursor == None:
                    self.cursor = self.connection.cursor()

                self.cursor.execute(sql, data)
                self.commit_ctr += 1

                if self.commit_ctr >= self.commit_at:
                    self.commit_write()
                    self.logger.debug("committed data successfully")

                return True

            except Exception as e:

                try:  # try not to lose data currently in cursor
                    self.commit_write()
                    if self.commit_ctr > 0:
                        self.logger.info("saved data that was pending commit! :)")
                except:
                    self.logger.critical("we couldn't save the data, doctor :(")

                self.logger.warning(f"write threw: {e}")
                self.logger.debug(json.dumps(data))
                retry_count = retry_count + 1

                if retry_count >= self.max_retries:
                    break

                time.sleep(5 * retry_count)

                self.soft_restart_connection()

        self.logger.error(f"exceeded retries, write failed with: {e}")
        return False

    def commit_write(self):
        if self.cursor is not None:
            self.connection.commit()
            self.cursor.close()
            self.cursor = None
            self.commit_ctr = 0
