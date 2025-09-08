-- Panthyr Database Schema
-- Generated from p_db_definitions.py
-- Authors: Dieter Vansteenwegen
-- Institution: VLIZ (Vlaams Instituut voor de Zee)

-- Create measurements table
CREATE TABLE IF NOT EXISTS measurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATE DEFAULT (datetime('now', 'utc')),
    valid TEXT DEFAULT 'n' COLLATE NOCASE,
    setup_error TEXT COLLATE NOCASE,
    cycle_id TEXT,
    gnss_acquired DATE,
    gnss_qual INTEGER,
    gnss_lat REAL,
    gnss_lon REAL,
    batt_voltage REAL,
    head_voltage REAL,
    head_temp_hpt TEXT,
    top_temperature REAL,
    top_humidity INTEGER,
    bottom_temperature REAL,
    bottom_humidity INTEGER,
    cycle_scan INTEGER,
    prot_sensor TEXT,
    prot_zenith INTEGER,
    prot_azimuth INTEGER,
    sun_heading REAL,
    sun_elevation REAL,
    scan_heading REAL,
    scan_error TEXT,
    scan_rep INTEGER,
    rep_error TEXT,
    rep_unix REAL,
    rep_serial TEXT,
    -- Add 256 measurement value columns (val_001 to val_256)
    val_001 REAL, val_002 REAL, val_003 REAL, val_004 REAL, val_005 REAL, val_006 REAL, val_007 REAL, val_008 REAL,
    val_009 REAL, val_010 REAL, val_011 REAL, val_012 REAL, val_013 REAL, val_014 REAL, val_015 REAL, val_016 REAL,
    val_017 REAL, val_018 REAL, val_019 REAL, val_020 REAL, val_021 REAL, val_022 REAL, val_023 REAL, val_024 REAL,
    val_025 REAL, val_026 REAL, val_027 REAL, val_028 REAL, val_029 REAL, val_030 REAL, val_031 REAL, val_032 REAL,
    val_033 REAL, val_034 REAL, val_035 REAL, val_036 REAL, val_037 REAL, val_038 REAL, val_039 REAL, val_040 REAL,
    val_041 REAL, val_042 REAL, val_043 REAL, val_044 REAL, val_045 REAL, val_046 REAL, val_047 REAL, val_048 REAL,
    val_049 REAL, val_050 REAL, val_051 REAL, val_052 REAL, val_053 REAL, val_054 REAL, val_055 REAL, val_056 REAL,
    val_057 REAL, val_058 REAL, val_059 REAL, val_060 REAL, val_061 REAL, val_062 REAL, val_063 REAL, val_064 REAL,
    val_065 REAL, val_066 REAL, val_067 REAL, val_068 REAL, val_069 REAL, val_070 REAL, val_071 REAL, val_072 REAL,
    val_073 REAL, val_074 REAL, val_075 REAL, val_076 REAL, val_077 REAL, val_078 REAL, val_079 REAL, val_080 REAL,
    val_081 REAL, val_082 REAL, val_083 REAL, val_084 REAL, val_085 REAL, val_086 REAL, val_087 REAL, val_088 REAL,
    val_089 REAL, val_090 REAL, val_091 REAL, val_092 REAL, val_093 REAL, val_094 REAL, val_095 REAL, val_096 REAL,
    val_097 REAL, val_098 REAL, val_099 REAL, val_100 REAL, val_101 REAL, val_102 REAL, val_103 REAL, val_104 REAL,
    val_105 REAL, val_106 REAL, val_107 REAL, val_108 REAL, val_109 REAL, val_110 REAL, val_111 REAL, val_112 REAL,
    val_113 REAL, val_114 REAL, val_115 REAL, val_116 REAL, val_117 REAL, val_118 REAL, val_119 REAL, val_120 REAL,
    val_121 REAL, val_122 REAL, val_123 REAL, val_124 REAL, val_125 REAL, val_126 REAL, val_127 REAL, val_128 REAL,
    val_129 REAL, val_130 REAL, val_131 REAL, val_132 REAL, val_133 REAL, val_134 REAL, val_135 REAL, val_136 REAL,
    val_137 REAL, val_138 REAL, val_139 REAL, val_140 REAL, val_141 REAL, val_142 REAL, val_143 REAL, val_144 REAL,
    val_145 REAL, val_146 REAL, val_147 REAL, val_148 REAL, val_149 REAL, val_150 REAL, val_151 REAL, val_152 REAL,
    val_153 REAL, val_154 REAL, val_155 REAL, val_156 REAL, val_157 REAL, val_158 REAL, val_159 REAL, val_160 REAL,
    val_161 REAL, val_162 REAL, val_163 REAL, val_164 REAL, val_165 REAL, val_166 REAL, val_167 REAL, val_168 REAL,
    val_169 REAL, val_170 REAL, val_171 REAL, val_172 REAL, val_173 REAL, val_174 REAL, val_175 REAL, val_176 REAL,
    val_177 REAL, val_178 REAL, val_179 REAL, val_180 REAL, val_181 REAL, val_182 REAL, val_183 REAL, val_184 REAL,
    val_185 REAL, val_186 REAL, val_187 REAL, val_188 REAL, val_189 REAL, val_190 REAL, val_191 REAL, val_192 REAL,
    val_193 REAL, val_194 REAL, val_195 REAL, val_196 REAL, val_197 REAL, val_198 REAL, val_199 REAL, val_200 REAL,
    val_201 REAL, val_202 REAL, val_203 REAL, val_204 REAL, val_205 REAL, val_206 REAL, val_207 REAL, val_208 REAL,
    val_209 REAL, val_210 REAL, val_211 REAL, val_212 REAL, val_213 REAL, val_214 REAL, val_215 REAL, val_216 REAL,
    val_217 REAL, val_218 REAL, val_219 REAL, val_220 REAL, val_221 REAL, val_222 REAL, val_223 REAL, val_224 REAL,
    val_225 REAL, val_226 REAL, val_227 REAL, val_228 REAL, val_229 REAL, val_230 REAL, val_231 REAL, val_232 REAL,
    val_233 REAL, val_234 REAL, val_235 REAL, val_236 REAL, val_237 REAL, val_238 REAL, val_239 REAL, val_240 REAL,
    val_241 REAL, val_242 REAL, val_243 REAL, val_244 REAL, val_245 REAL, val_246 REAL, val_247 REAL, val_248 REAL,
    val_249 REAL, val_250 REAL, val_251 REAL, val_252 REAL, val_253 REAL, val_254 REAL, val_255 REAL, val_256 REAL
);

-- Create queue table
CREATE TABLE IF NOT EXISTS queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    done INTEGER NOT NULL DEFAULT 0 COLLATE NOCASE,
    priority INTEGER NOT NULL DEFAULT 2,
    fails INTEGER NOT NULL DEFAULT 0,
    timestamp DATE DEFAULT (datetime('now', 'utc')),
    action TEXT NOT NULL COLLATE NOCASE,
    options TEXT DEFAULT NULL COLLATE NOCASE
);

-- Create protocol table
CREATE TABLE IF NOT EXISTS protocol (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    number INTEGER NOT NULL UNIQUE,
    instrument TEXT NOT NULL COLLATE NOCASE,
    zenith INTEGER NOT NULL,
    azimuth INTEGER NOT NULL,
    repeat INTEGER NOT NULL DEFAULT 1,
    wait INTEGER NOT NULL DEFAULT 0
);

-- Create logs table
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATE DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    level TEXT COLLATE NOCASE,
    source TEXT NOT NULL COLLATE NOCASE,
    log TEXT DEFAULT NULL COLLATE NOCASE
);

-- Create settings table
CREATE TABLE IF NOT EXISTS settings (
    setting TEXT PRIMARY KEY NOT NULL COLLATE NOCASE,
    value TEXT COLLATE NOCASE
);

-- Insert default settings
INSERT OR IGNORE INTO settings (setting, value) VALUES
('station_id', 'MSO'),
('manual', '1'),
('system_set_up', '0'),
('gnss_acquired', 'none'),
('gnss_lat', '51.2'),
('gnss_lon', '2.9'),
('gnss_qual', '0'),
('gnss_mag_var', '0'),
('measurements_start_hour', '6'),
('measurements_stop_hour', '19'),
('max_sun_zenith', '90'),
('email_enabled', '1'),
('email_recipient', ''),
('email_server_port', ''),
('email_user', ''),
('email_password', ''),
('email_min_level', 'warning'),
('ftp_server', ''),
('ftp_user', ''),
('ftp_password', ''),
('ftp_working_dir', '.'),
('head_true_north_offset', '180'),
('radiance_angle_offset', '20'),
('irradiance_angle_offset', '60'),
('keepout_heading_low', '0'),
('keepout_heading_high', '0'),
('id_last_backup_meas', '0'),
('id_last_backup_log', '0'),
('tty_irradiance', '/dev/ttyO1'),
('tty_radiance', '/dev/ttyO2'),
('tty_multiplexer', '/dev/ttyO5'),
('tty_gnss', '/dev/ttyO4'),
('adc_channel', '');

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_measurements_timestamp ON measurements(timestamp);
CREATE INDEX IF NOT EXISTS idx_queue_done_priority ON queue(done, priority);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_protocol_number ON protocol(number);
