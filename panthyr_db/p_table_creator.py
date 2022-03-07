#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Authors: Dieter Vansteenwegen
# Institution: VLIZ (Vlaams Instituut voor de Zee)

__author__ = 'Dieter Vansteenwegen'
__email__ = 'dieter.vansteenwegen@vliz.be'
__project__ = 'Panthyr'
__project_link__ = 'https://waterhypernet.org/equipment/'

from typing import Union
from .p_db import DATABASE_LOCATION
from .p_db import pDB
from . import p_db_definitions as defs
from os import path
import logging
import sys

VALID_TABLES = ('protocol', 'settings', 'measurements', 'queue', 'logs')


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


class pTableCreator():

    def __init__(self,
                 db_file: str = DATABASE_LOCATION,
                 table_id: tuple = ('all', ),
                 populate_settings: bool = True,
                 owner: Union[tuple, None] = None):
        """_summary_

        _extended_summary_

        Args:
            db_file (str, optional): _description_. Defaults to DATABASE_LOCATION.
            table_id (tuple, optional): _description_. Defaults to ('all', ).
            populate_settings (bool, optional): _description_. Defaults to True.
            ownership (Union[tuple, None], optional): _description_. Defaults to None.
        """

        self.log = initialize_logger()
        self.db_file = db_file

        self._check_if_file_exists()
        self._check_table_list(table_id)
        self._tables_to_generate = table_id if table_id != ('all', ) else VALID_TABLES
        self.populate_settings = populate_settings
        self._create_db()
        self.db.close()

        if owner:
            self._change_file_ownership(owner)

    def _check_if_file_exists(self):
        if path.isfile(self.db_file):
            self.log.error(
                f'The file {self.db_file} exists on disk. Not doing anything.\n Quitting now...')
            sys.exit()

    def _check_table_list(self, table_id: tuple):
        for t in table_id:
            if t != 'all' and t not in VALID_TABLES:
                err_msg = f'{t} is not a valid table name. Valid: \'all\' or {VALID_TABLES}'
                raise Exception(err_msg)

    def _create_db(self):
        self.db = pDB(self.db_file)
        self._create_tables()
        self.db.commit()

    def _create_tables(self):
        self._add_protocol_table()
        self._add_logs_table()
        self._add_queue_table()
        self._add_measurements_table()
        self._add_settings_table()

    def _generate_create_command(self, table):
        cols: tuple = ()

        if table == 'measurements':
            cols = defs.MEASUREMENTS_TABLE
        elif table == 'queue':
            cols = defs.QUEUE_TABLE
        elif table == 'protocol':
            cols = defs.PROTOCOL_TABLE
        elif table == 'logs':
            cols = defs.LOGS_TABLE
        elif table == 'settings':
            cols = defs.SETTINGS_TABLE

        base_command = f'create table {table}('
        columns_command = ''.join(f'{column}, ' for column in cols)
        return f'{base_command}{columns_command[:-2]})'

    def _add_protocol_table(self):
        if 'protocol' in self._tables_to_generate:
            command = self._generate_create_command('protocol')
            with self.db:
                self.db.execute(command)

    def _add_measurements_table(self):
        if 'measurements' in self._tables_to_generate:
            command = self._generate_create_command('measurements')
            command = f'{command[:-1]}, '
            # Add columns for the 256 values
            for i in range(1, 257):
                command += f'val_{i:03d} integer, '
            # remove last ', ' and add closing brackets
            command = f'{command[:-2]})'
            with self.db:
                self.db.execute(command)

    def _add_queue_table(self):
        if 'queue' in self._tables_to_generate:
            command = self._generate_create_command('queue')
            with self.db:
                self.db.execute(command)

    def _add_logs_table(self):
        if 'logs' in self._tables_to_generate:
            command = self._generate_create_command('logs')
            with self.db:
                self.db.execute(command)

    def _add_settings_table(self):
        if 'settings' in self._tables_to_generate:
            command = self._generate_create_command('settings')
            with self.db:
                self.db.execute(command)
            if self.populate_settings:
                self._populate_settings()

    def _populate_settings(self):
        with self.db:
            self.db.executemany('insert into settings(setting,value) values (?, ?)',
                                (defs.DEFAULT_SETTINGS))

    def _change_file_ownership(self, owner: tuple):
        import pwd
        import grp
        import os

        try:
            uid = pwd.getpwnam(owner[0]).pw_uid
            gid = grp.getgrnam(owner[1]).gr_gid
        except KeyError:
            self.log.exception(
                f'Could not get uid/gid for {owner[0]}/{owner[1]}. Not setting uid/gid.')
            raise
        try:
            os.chown(self.db_file, uid, gid)  # noqa
        except OSError:
            self.log.exception(f'Could not set uid {uid}/gid {gid} for file {self.db_file}')
            raise
