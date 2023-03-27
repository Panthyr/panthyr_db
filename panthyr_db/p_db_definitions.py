# -*- coding: utf-8 -*-

DATABASE_LOCATION = '/home/panthyr/data/panthyr.db'
# Values that are stored for each measurement.
# If these change, also update MEASUREMENTS_TABLE!
MEASUREMENTS_STORED = (
    'timestamp',
    'valid',
    'setup_error',
    'cycle_id',
    'gnss_acquired',
    'gnss_qual',
    'gnss_lat',
    'gnss_lon',
    'batt_voltage',
    'head_voltage',
    'head_temp_hpt',
    'cycle_scan',
    'prot_sensor',
    'prot_zenith',
    'prot_azimuth',
    'sun_heading',
    'sun_elevation',
    'scan_heading',
    'scan_error',
    'scan_rep',
    'rep_error',
    'rep_unix',
    'rep_serial',
)

MEASUREMENTS_TABLE = (
    'id integer primary key autoincrement',
    'timestamp date default (datetime(\'now\', \'utc\'))',
    'valid text default \'n\' collate nocase',
    'setup_error text collate nocase',
    'cycle_id text',
    'gnss_acquired date',
    'gnss_qual integer',
    'gnss_lat real',
    'gnss_lon real',
    'batt_voltage real',
    'head_voltage real',
    'head_temp_hpt text',
    'cycle_scan integer',
    'prot_sensor text',
    'prot_zenith integer',
    'prot_azimuth integer',
    'sun_heading real',
    'sun_elevation real',
    'scan_heading real',
    'scan_error text',
    'scan_rep integer',
    'rep_error text',
    'rep_unix real',
    'rep_serial text',
)

QUEUE_TABLE = (
    'id integer primary key autoincrement',
    'done integer not null default 0 collate nocase',
    'priority integer not null default 2',
    'fails integer NOT NULL DEFAULT 0',
    'timestamp date default (datetime(\'now\', \'utc\'))',
    'action text not null collate nocase',
    'options text default null collate nocase',
)

PROTOCOL_TABLE = (
    'id integer primary key autoincrement',
    'number integer not null unique',
    'instrument text not null collate nocase',
    'zenith integer not null',
    'azimuth integer not null',
    'repeat integer not null default 1',
    'wait integer not null default 0',
)

LOGS_TABLE = (
    'id integer primary key autoincrement',
    "timestamp date default (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))",
    'level text collate nocase',
    'source text not null collate nocase',
    'log text default null collate nocase',
)

SETTINGS_TABLE = ('setting text primary key not null collate nocase', 'value text collate nocase')

DEFAULT_SETTINGS = (
    ('station_id', 'MSO'),
    ('manual', 1),
    ('measurements_start_hour', 6),
    ('measurements_stop_hour', 19),
    ('max_sun_zenith', 90),
    ('email_enabled', 1),
    ('email_recipient', ''),
    ('email_server_port', ''),
    ('email_user', ''),
    ('email_password', ''),
    ('email_min_level', 'warning'),
    ('ftp_server', ''),
    ('ftp_user', ''),
    ('ftp_password', ''),
    ('ftp_working_dir', '.'),
    (
        'head_true_north_offset',
        180,
    ),
    (
        'radiance_angle_offset',
        20,
    ),
    (
        'irradiance_angle_offset',
        60,
    ),
    ('keepout_heading_low', 0),
    (
        'keepout_heading_high',
        0,
    ),
    ('gnss_acquired', 'none'),
    ('gnss_lat', 51.2),
    ('gnss_lon', 2.9),
    ('gnss_qual', 0),
    ('gnss_mag_var', 0),
    ('id_last_backup_meas', 0),
    ('id_last_backup_log', 0),
    ('system_set_up', 0),
    ('tty_irradiance', '/dev/ttyO1'),
    ('tty_radiance', '/dev/ttyO2'),
    ('tty_multiplexer', '/dev/ttyO5'),
    ('tty_gnss', '/dev/ttyO4'),
    ('adc_channel', ''),
)
