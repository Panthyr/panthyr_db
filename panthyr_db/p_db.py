#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Authors: Dieter Vansteenwegen
# Institution: VLIZ (Vlaams Instituut voor de Zee)
import contextlib

__author__ = 'Dieter Vansteenwegen'
__email__ = 'dieter.vansteenwegen@vliz.be'
__project__ = 'Panthyr'
__project_link__ = 'https://waterhypernet.org/equipment/'

from typing import List, Optional, Tuple, Union
import sqlite3  # Because ... Well...
import logging

from panthyr_db.p_table_creator import pTableCreator
from . import p_db_definitions as defs

CREDENTIALS_FILE: str = '/home/panthyr/data/credentials'
DEFAULT_CREDENTIALS: tuple = (
    'email_user',
    'email_password',
    'email_server_port',
    'ftp_server',
    'ftp_user',
    'ftp_password',
)


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

    def __init__(self, database: str = defs.DATABASE_LOCATION, **kwargs):
        """_summary_

        _extended_summary_

        Args:
            database (str, optional): filename (including path of db).
                        Defaults to defs.DATABASE_LOCATION.
        """
        super(pDB, self).__init__(database=database, **kwargs)
        self._c = self.cursor()  # cursor object
        self.log = initialize_logger()
        self.db_filename = database

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

        self.execute(
            'insert into queue(priority, action, options) values (? ,? ,?)',
            (priority, task, options),
        )
        self._commit_db()

    def get_next_task(self, only_high_priority: bool = False) -> Union[None, tuple]:
        """Checks the db for tasks.

        Queries the "queue" table in the database_location db for tasks where done = "0".
        Db is first queried for tasks with priority "1". If no tasks with high priority are found,
        and if only_high_priority is False, then priority "2" tasks are queried.

        Sorting is done by priority and then id (thus order of creation).
        Returns the next job (if any) as a tuple (id, priority, action, options, fails).

        Priority col values: 1 = high priority (manually queued, ...), 2 = normal priority
        Done col values: 1 = done, 0 = to be done

        Args:
            only_high_priority (bool, optional): only return tasks with priority 1.
                                                Defaults to False.

        Returns:
            Union[None, tuple]: a tuple (id, priority, action, options, fails) if
                                a task was returned, None if not.
        """

        cmd_template = (
            'select id, priority, action, options, fails from queue where'
            ' done == 0 and priority == {priority} and fails < 3 order by id limit 1'
        )

        # try to get task with priority 1
        self._c.execute(cmd_template.format(priority=1))
        reply = self._c.fetchone()

        if type(reply) == tuple:
            return reply

        if not only_high_priority:
            # No tasks with priority 1
            self._c.execute(cmd_template.format(priority=2))
            reply = self._c.fetchone()
            return reply if type(reply) == tuple else None

    def get_number_of_tasks(self, only_high_priority: bool = False) -> int:
        """Checks the db for tasks and returns how many are to be done.

        Queries the 'queue' table in the database_location db for the number of tasks where
            done = '0' and fails < '3'.
        If only_high_priority is True, only queries for tasks with priority 1.

        Args:
            only_high_priority (bool, optional): only return number of tasks with priority 1.
                                                Defaults to False.

        Returns:
            int: number of undone tasks
        """

        cmd = 'select count() from queue where done == 0 and fails < 3'
        if only_high_priority:
            cmd += ' and priority == 1'

        self._c.execute(cmd)
        try:
            reply = self._c.fetchone()[0]  # fetchone returns a tuple ()
        except IndexError:
            reply = 0

        return int(reply)

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
            query = f'select fails from queue where id == {id}'  # nosec B608
            self._c.execute(query)
            fails = int(self._c.fetchone()[0])
            self._c.execute('update queue set fails = ? where id == ?', (fails + 1, id))
        else:
            self._c.execute("update queue set done = '1' where id == ?", (id,))
        self._commit_db()

    def get_setting(self, setting: str) -> Union[str, int, float, None]:
        """Return the value of a setting in the 'settings' table.

        Args:
            setting (str): setting id to be fetched.

        Returns:
            Union[str, int, None]: integer if the setting is an int,
                                    str if setting is a string,
                                    None if setting is not in db.
        """

        try:
            self._c.execute('SELECT value FROM settings WHERE setting = ?', (setting,))
            reply = self._c.fetchone()[0]

        except TypeError:
            err_str = f'Error while getting setting for {setting}, is setting in db?'
            self.log.warning(err_str)
            return None

        try:
            reply = int(reply)
        except ValueError:
            with contextlib.suppress(TypeError, ValueError):
                reply = float(reply)
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
            self._c.execute('insert or ignore into settings(setting) VALUES(?)', (setting,))
            self._c.execute('update settings set value = ? where setting = ?', (value, setting))
            self._commit_db()

        except Exception:
            err_str = f'Error while setting setting {setting} to {value}'
            self.log.exception(err_str)
            raise

    def get_protocol(self) -> List[dict]:
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
        return [
            {
                'id': i,
                'instrument': s[0].lower(),
                'zenith': s[1],
                'azimuth': s[2],
                'repeat': s[3],
                'wait': s[4],
            } for i, s in enumerate(response, start=1)
        ]

    def add_log(self, logtext: str, source: str = 'none', level: str = 'info'):
        """Adds logtext into the logs table.

        Args:
            logtext (str): log text
            source (str, optional): module name. Defaults to 'none'.
            level (str, optional): severity. Defaults to 'info'.
        """
        self.execute(
            'insert into logs(level, source, log) values (?, ?, ?)',
            (level, source, logtext),
        )
        self._commit_db()

    def get_last_id(self, table: str) -> Union[int, None]:
        """Return the last/highest id in [table].

        Args:
            table (str): table to get id from.

        Returns:
            Union[int,None]: highest id or None if table is empty
        """

        self._c.execute(f'SELECT MAX(id) FROM {table}')  # nosec B608
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
        assert type(meas_dict) == dict  # nosecB101
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

    def _measurement_command(self, meas_dict_clean: dict) -> Tuple[str, list]:
        """Generate the SQLite command to store the measurement dict.

        Args:
            meas_dict_clean (dict): data to be stored in the measurement table

        Returns:
            Tuple[str, list]: command for SQLite and the list of values.
        """
        columns = ''
        placeholders = ''
        values = []
        for i in meas_dict_clean:  # sourcery skip: replace-dict-items-with-values, use-dict-items
            columns += f'{i}, '
            placeholders += '?, '
            values.append(meas_dict_clean[i])

        cmd = f'insert into measurements({columns[:-2]}) values ({placeholders[:-2]})'

        return (cmd, values)

    def export_data(
        self,
        target_db: str,
        table_ids: Tuple[Tuple[str, Optional[int], Optional[int]]],
    ) -> None:
        """Exports data from selected tables and ranges to new database.

        Args:
            target_db (str): full path/filename of target db
            table_ids (Tuple[Tuple[str, Optional[int], Optional[int]]]): a tuple of tuples,
                                one for each table that needs to be exporting.
                                optionally, provide start and stop id to export
        """
        if not table_ids:
            self.log.warning('no tables have been specified')
            return
        if any(type(i) not in (tuple, list) for i in table_ids):
            self.log.error('Invalid argument passed for table_ids: should be a tuple of tuples')
            return
        self._create_export_target(target_db=target_db, table_ids=table_ids)
        self._export_to_target(target_db=target_db, table_ids=table_ids)

    def _create_export_target(
        self,
        target_db: str,
        table_ids: Tuple[Tuple[str, Optional[int], Optional[int]]],
    ) -> None:
        """Create a blank database to export to.

        Args:
            target_db (str): full path/filename of target db
            table_ids (Tuple[Tuple[str, Optional[int], Optional[int]]]): a tuple of tuples,
                                one for each table that needs to be exporting.
                                optionally, provide start:int and stop:int to export (ignored here)
        """
        tables = tuple(i[0] for i in table_ids)
        pTableCreator(db_file=target_db, tables=tables)

    def _export_to_target(
        self,
        target_db: str,
        table_ids: Tuple[Tuple[str, Optional[int], Optional[int]]],
    ) -> None:
        """Export the requested tables/ranges to the new database.

        For each tuple in the table_ids, export the table to the new database.
        If start is defined, only export id's higher than start id.
        if start and stop are defined, only export range between id's

        Args:
            target_db (str): full path/filename of target db
            table_ids (Tuple[Tuple[str, Optional[int], Optional[int]]]): a tuple of tuples,
                                one for each table that needs to be exporting.
                                optionally, provide start:int and stop:int to export
        """
        self.execute('ATTACH DATABASE ? AS target_db', (target_db,))
        for table in table_ids:
            cmd = self._generate_export_cmd(table)
            if cmd:
                if type(cmd) == str:
                    self.execute(cmd)
                else:
                    self.execute(cmd[0], cmd[1])
        self._commit_db()
        self.execute('DETACH DATABASE "target_db"')

    def _generate_export_cmd(
        self,
        table: Tuple[str, Optional[int], Optional[int]],
    ) -> Union[Tuple[str, list], str, None]:
        """Generate command to export table.

        If start id is given, export starting at that id
        If stop id is given, export until that id -1

        Args:
            table (Tuple[str, Optional[int], Optional[int]]): table name, start id, stop id

        Returns:
            Union[Tuple[str, list], str, None]: None if the id's are not correct
                                        str if the command does not include substitutions
                                        tuple if a command and substitution is necessary.
        """
        table_name = table[0]
        start = table[1] if len(table) > 1 else None
        stop = table[2] if len(table) > 2 else None

        cmd = f'INSERT INTO target_db.{table_name} SELECT * FROM {table_name}'  #nosecB608

        if start or stop:  # not all ids need to be exported
            substitution = []
            cmd += ' WHERE id '
            if start and stop:
                if not start < stop:
                    self.log.warning(
                        f'trying to export {table}, but end id {stop} '
                        f'is not lower than start_id ({start}). '
                        f'Skipping this table', )
                    return None
                cmd += 'BETWEEN ? AND ?'
                substitution += [start - 1, stop]
            elif start:
                cmd += '> ?'
                substitution += [
                    start - 1,
                ]
            elif stop:
                cmd += '< ?'
                substitution += [
                    stop,
                ]
            return (cmd, substitution)
        else:
            return cmd

    def populate_credentials(
        self,
        credentials_file: str = CREDENTIALS_FILE,
        credentials: tuple = DEFAULT_CREDENTIALS,
    ):
        # TODO
        pass
