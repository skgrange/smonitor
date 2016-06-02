-- Create tables, order matters due to indices
CREATE TABLE sites (
  site TEXT PRIMARY KEY,
  site_name TEXT,
  latitude REAL,
  longitude REAL,
  elevation REAL,
  address TEXT,
  region TEXT,
  site_type TEXT,
  operator TEXT,
  notes TEXT
);

CREATE TABLE processes (
  process INTEGER PRIMARY KEY,
  site TEXT REFERENCES sites(site),
  variable TEXT NOT NULL,
  variable_long TEXT,
  numeric INTEGER,
  period TEXT,
  unit TEXT,
  method TEXT,
  primary_identifier TEXT,
  secondary_identifier INTEGER,
  date_start TEXT,
  date_end TEXT,
  compliant_monitoring INTEGER,
  priority INTEGER,
  notes TEXT
);

CREATE TABLE aggregations (
  summary INTEGER PRIMARY KEY,
  summary_name TEXT,
  source TEXT,
  period TEXT,
  aggregation_function TEXT,
  notes TEXT
);

CREATE TABLE summaries (
  process INTEGER REFERENCES processes(process),
  summary INTEGER REFERENCES aggregations(summary),
  site TEXT,
  variable TEXT,
  summary_name TEXT,
  validity_threshold INTEGER,
  source TEXT,
  period TEXT,
  aggregation_function TEXT,
  notes TEXT
);

CREATE TABLE observations (
  date_insert INTEGER,
  date INTEGER NOT NULL,
  date_end INTEGER,
  process INTEGER REFERENCES processes(process),
  summary INTEGER REFERENCES aggregations(summary),
  validity INTEGER,
  value REAL
);

CREATE TABLE invalidations (
  process INTEGER REFERENCES processes(process),
  site TEXT,
  variable TEXT,
  date_start TEXT,
  date_end TEXT,
  notes TEXT
);

-- Create indices for observations table, slows insersion and bloats size but
-- makes queries fast
CREATE INDEX index_observations_process ON observations(process);
CREATE INDEX index_observations_summary ON observations(summary);
