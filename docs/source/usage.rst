===============================
Panthyr DB example code
===============================

Example code generating an empty db:

.. code:: python

    >>> from panthyr_db.p_table_creator import pTableCreator
    # Create empty db
    >>> db = pTableCreator(db_file = './new_empty.db', tables = ('all',),
                        owner = ('hypermaq', 'hypermaq'))


Example code interacting with a db:

.. code:: python

    >>> from panthyr_db.p_db import pDB
    >>> db = pDB('./example.db')

    # Settings
    #
    >>> db.get_setting('station_id')
    'RT1'
    >>> db.set_setting('test_setting', 'test_setting_value')
    >>> db.get_setting('test_setting')
    'test_setting_value'


    # Handling tasks
    #
    # add a task to the queue
    >>> db.add_to_queue('measure', 1, 'test')
    >>> db.get_number_of_tasks()
    1
    >>> db.get_next_task()
    (28489, 1, 'measure', 'test', 0)
    # Oh no, task failed the first time
    >>> db.set_task_handled(28489, failed = True)
    >>> db.get_next_task(only_high_priority = True)
    (28489, 1, 'measure', 'test', 1)
    # Failed counter has incremented
    >>> db.set_task_handled(28489, failed = False)
    >>> db.get_next_task()
    # No task returned, all done.
    >>> db.get_last_id('queue')
    28487

    # Add a log
    #
    >>> db.add_log('this is the logged text', 'source_module', 'debug')

    # Get the measurement protocol
    #
    >>> db.get_protocol()[0]
    {'id': 1, 'instrument': 'e', 'zenith': 180, 'azimuth': 90, 'repeat': 3, 'wait': 0}

    # Export data to a non-existing database backup.db.
    # Export measurements starting at id 10 and logs with id's between 50 and 60
    >>> db.export_data('./backup.db', (('measurements',10),('logs',50,60)))
