#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Authors: Dieter Vansteenwegen
# Institution: VLIZ (Vlaams Instituut voor de Zee)

__author__ = 'Dieter Vansteenwegen'
__email__ = 'dieter.vansteenwegen@vliz.be'
__project__ = 'Panthyr'
__project_link__ = 'https://waterhypernet.org/equipment/'

import sqlite3  # Because ... Well...
import logging

DATABASE_LOCATION = '/home/hypermaq/data/hypermaq.db'
CREDENTIALS_FILE: str = '/home/hypermaq/data/credentials'
DEFAULT_CREDENTIALS: tuple = ('email_user', 'email_password', 'email_server_port', 'ftp_server',
                              'ftp_user', 'ftp_password')


def initialize_logger() -> logging.Logger:
    """Set up logger
    If the module is ran as a module, name logger accordingly as a sublogger.
    Returns:
        logging.Logger: logger instance
    """
    if __name__ == '__main__':
        return logging.getLogger('{}'.format(__name__))
    else:
        return logging.getLogger('__main__.{}'.format(__name__))


class pDB(sqlite3.Connection):

    def __init__(self, database=DATABASE_LOCATION, **kwargs):
        super(pDB, self).__init__(database=database, **kwargs)
        self._c = self.cursor()  # cursor object
        self.log = initialize_logger()

    def _commit_db(self):
        self.commit()

    def _close_db(self):
        self.close()

    def vacuum_db(self):
        """Commit changes to the database, then vacuum."""
        try:
            self._commit_db()
            self.execute('VACUUM')

        except Exception:
            self.log.exception('Exception vacuuming db')
            raise

    def add_to_queue(self, action: str, priority: int = 2, options: str = ''):
        """Add a task to the queue table."""
        pass

    def get_next_task(self):
        pass

    def set_task_handled(self, id: int, failed: bool = False, fails: int = 0):
        pass

    def get_setting(self, setting: str) -> str:
        """Return the value of a setting in the 'settings' table."""
        pass

    def set_setting(self, setting: str, value: str):
        """Set setting in the 'settings' table."""
        pass

    def get_protocol(self) -> list[dict]:
        pass

    def add_log(self, logtext: str, source: str = 'none', level: str = 'info'):
        pass

    def add_measurement(self, meas_dict: dict):
        pass

    def get_last_id(self, table: str):
        pass

    def export_data(self, target_db_name: str, table_ids: list):
        pass

    def populate_credentials(self,
                             credentials_file: str = CREDENTIALS_FILE,
                             credentials: tuple = DEFAULT_CREDENTIALS):
        pass
