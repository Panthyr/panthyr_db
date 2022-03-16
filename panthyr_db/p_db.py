#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Authors: Dieter Vansteenwegen
# Institution: VLIZ (Vlaams Instituut voor de Zee)

__author__ = 'Dieter Vansteenwegen'
__email__ = 'dieter.vansteenwegen@vliz.be'
__project__ = 'Panthyr'
__project_link__ = 'https://waterhypernet.org/equipment/'

from typing import Union
import sqlite3  # Because ... Well...
import logging
from . import p_db_definitions as defs

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

    def __init__(self, database: str = DATABASE_LOCATION, **kwargs):
        """_summary_

        _extended_summary_

        Args:
            database (str, optional): filename (including path of db). 
                        Defaults to DATABASE_LOCATION.
        """
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

    def add_to_queue(self, task: str, priority: int = 2, options: str = ''):
        """Add a task to the queue table.

        Args:
            task (str): task to be added
            priority (int, optional): priority, 1 or 2. Defaults to 2.
            options (str, optional): option field (arguments). Defaults to ''.
        """

        self.execute('insert into queue(priority, action, options) values (? ,? ,?)',
                     (priority, task, options))
        self._commit_db()

    def get_next_task(self) -> Union[None, tuple]:
        """Checks the db for tasks.

        Queries the "queue" table in the database_location db for tasks where done = "0".
        Db is first queried for tasks with priority "1", then priority "2".
        Sorting is done by id (thus order of creation).
        Returns the next job (if any) as a tuple (id, priority, action, options, fails).

        Priority col values: 1 = high priority (manually queued, ...), 2 = normal priority
        Done col values: 1 = done, 0 = to be done

        Returns:
            Union[None, tuple]: a tuple if a task was returned, None if not.
        """

        cmd_template = ('select id, priority, action, options, fails from queue where'
                        ' done == 0 and priority == {priority} and fails < 3 order by id limit 1')

        # try to get task with priority 1
        self._c.execute(cmd_template.format(priority=1))
        reply = self._c.fetchone()

        if type(reply) == tuple:
            return reply

        # No tasks with priority 1
        self._c.execute(cmd_template.format(priority=2))
        reply = self._c.fetchone()
        return reply if type(reply) == tuple else None

    def set_task_handled(self, id: int, failed: bool = False):
        """Marks a task in the queue table as done (or adds to the fail counter).

        Takes the task id as argument.
        If task has failed, the current number of fails is queried and incremented by one.

        Args:
            id (int): task id
            failed (bool, optional): task has failed. 
                    Do not set done field but increment fails. 
                    Defaults to False.

        Raises:
            TypeError: if one of the arguments is of an incorrect type.
        """

        if type(id) != int or id < 0:  # check if a valid id is passed
            msg = f'No valid queue ID provided ({id})'
            self.log.warning(msg)
            raise TypeError(msg)

        if failed:
            query = f'select fails from queue where id == {id}'
            self._c.execute(query)
            fails = int(self._c.fetchone()[0])
            self._c.execute('update queue set fails = ? where id == ?', (fails + 1, id))
        else:
            self._c.execute("update queue set done = '1' where id == ?", (id, ))
        self._commit_db()

    def get_setting(self, setting: str) -> Union[str, int, None]:
        """Return the value of a setting in the 'settings' table.

        Args:
            setting (str): setting id to be fetched.

        Returns:
            Union[str, int, None]: integer if the setting is an int, 
                                    str if setting is a string,
                                    None if setting is not in db.
        """

        try:
            self._c.execute('SELECT value FROM settings WHERE setting = ?', (setting, ))
            reply = self._c.fetchone()[0]

        except TypeError:
            err_str = f'Error while getting setting for {setting}, is setting in db?'
            self.log.exception(err_str)
            return None

        try:  # check if the value is an integer, if so return it as int
            return int(reply)
        except (TypeError, ValueError):
            return reply

    def set_setting(self, setting: str, value: str):
        """Adds or changes settings in the the 'settings' table.
        
        Updates setting in table if it is already available, else creates new id.

        Args:
            setting (str): setting to be set
            value (str): value to be set
        """

        try:
            # creates a new setting (row) if it doesn't exist, else does nothing
            self._c.execute('insert or ignore into settings(setting) VALUES(?)', (setting, ))
            self._c.execute('update settings set value = ? where setting = ?', (value, setting))
            self._commit_db()

        except Exception:
            err_str = f'Error while setting setting {setting} to {value}'
            self.log.exception(err_str)
            raise

    def get_protocol(self) -> list[dict]:
        """Get the protocol from the protocol table.

        Returns:
            list[dict]: list of one dict per measurement.
                    keys: id, instrument, zenith, azimuth, repeat, wait
        """

        cmd = 'SELECT instrument, zenith, azimuth, repeat, wait from protocol ORDER BY number'
        self._c.execute(cmd)

        response = self._c.fetchall()
        # Returns a list (1 item for each row) of tuples (each element representing one column)

        # TODO check return
        # TODO what if no protocol defined?
        return [{
            'id': i,
            'instrument': s[0].lower(),
            'zenith': s[1],
            'azimuth': s[2],
            'repeat': s[3],
            'wait': s[4]
        } for i, s in enumerate(response, start=1)]

    def add_log(self, logtext: str, source: str = 'none', level: str = 'info'):
        """Adds logtext into the logs table.

        Args:
            logtext (str): log text
            source (str, optional): module name. Defaults to 'none'.
            level (str, optional): severity. Defaults to 'info'.
        """
        self.execute('insert into logs(level, source, log) values (?, ?, ?)',
                     (level, source, logtext))
        self._commit_db()

    def get_last_id(self, table: str) -> Union[int, None]:
        """Return the last/highest id in [table].

        Args:
            table (str): table to get id from.

        Returns:
            Union[int,None]: highest id or None if table is empty
        """

        self._c.execute(f'SELECT MAX(id) FROM {table}')
        # TODO check what is returned if table is empty
        try:
            reply = self._c.fetchone()
        except sqlite3.OperationalError:
            err_str = f'Error getting last id from table {table}. Does table exist?'
            self.log.exception(err_str)
            raise

        try:
            rtn = int(reply[0])
        except TypeError:
            rtn = None

        return rtn

    def add_measurement(self, meas_dict: dict):
        """Sanitize and add measurement data to db

        p_db_definitions.MEASUREMENTS_STORED is used as a reference for all required fields.
        Fields not the given meas_dict are added as an empty string.
        
        Args:
            meas_dict (dict): measurement data, may not contain all required fields.
        """
        meas_dict_clean = self._cleanup_measurement(meas_dict)

        cmd, values = self._measurement_command(meas_dict_clean)

        self.execute(cmd, values)
        self._commit_db()

    def _cleanup_measurement(self, meas_dict: dict) -> dict:
        """Convert the given meas_dict to a default one.

        Check given dict and add all items from p_db_definitions.MEASUREMENTS_STORED as well as
        256 measurement items. If items are not available in the given dict, they are left empty.

        Args:
            meas_dict (dict): raw dictionnary to be cleaned

        Returns:
            dict: standard measurement dictionary with all required items
        """
        assert type(meas_dict) == dict
        stored_values = defs.MEASUREMENTS_STORED
        rtn_dict = {}

        # fill in all required items
        for var in stored_values:
            if var in meas_dict:
                # For scan/setup errors: convert list of errors to string
                if var in ('scan_error', 'setup_error'):
                    rtn_dict[var] = ' | '.join([str(x) for x in meas_dict[var]])
                else:
                    rtn_dict[var] = meas_dict[var]
            else:
                rtn_dict[var] = ''

        # add measurement data
        for i in range(1, 257):
            try:
                val = meas_dict['data'][i - 1]
            except (KeyError, IndexError):  # not enough data values or data doesn't exist
                val = ''
            rtn_dict[f'val_{i:03d}'] = val

        return rtn_dict

    def _measurement_command(self, meas_dict_clean: dict) -> tuple[str, list]:
        """Generate the SQLite command to store the measurement dict.

        Args:
            meas_dict_clean (dict): data to be stored in the measurement table

        Returns:
            tuple[str, list]: command for SQLite and the list of values.
        """
        columns = ''
        placeholders = ''
        values = []
        for i in meas_dict_clean:
            columns += 'f{i}, '
            placeholders += '?, '
            values.append(meas_dict_clean[i])

        cmd = f'insert into measurements({columns[:-2]}) values ({placeholders[:-2]})'

        return (cmd, values)

    def export_data(self, target_db_name: str, table_ids: list):
        # TODO
        pass

    def populate_credentials(self,
                             credentials_file: str = CREDENTIALS_FILE,
                             credentials: tuple = DEFAULT_CREDENTIALS):
        # TODO
        pass
