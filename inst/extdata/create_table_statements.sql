/* Create tables */

CREATE TABLE aggregations (
  "summary" integer,
  "summary_name" text,
  "source" text,
  "period" text,
  "aggregation_function" text,
  "notes" text
);
CREATE TABLE invalidations (
  "process" real,
  "site" text,
  "variable" text,
  "date_start" text,
  "date_end" text,
  "notes" text
);
CREATE TABLE observations (
  "date_insert" integer,
  "date" integer,
  "date_end" integer,
  "process" integer,
  "summary" integer,
  "validity" integer,
  "value" real
);
CREATE TABLE processes (
  "process" integer,
  "site" text,
  "variable" text,
  "variable_long" text,
  "numeric" integer,
  "period" text,
  "unit" text,
  "method" text,
  "primary_identifier" text,
  "secondary_identifier" integer,
  "date_start" text,
  "date_end" text,
  "compliant_monitoring" integer,
  "priority" integer,
  "notes" text
);
CREATE TABLE sites (
  "site" text,
  "site_name" text,
  "latitude" real,
  "longitude" real,
  "elevation" real,
  "address" text,
  "region" text,
  "site_type" text,
  "operator" text,
  "notes" text
);
CREATE TABLE summaries (
  "process" integer,
  "summary" integer,
  "site" text,
  "variable" text,
  "summary_name" text,
  "validity_threshold" integer,
  "source" text,
  "period" text,
  "aggregation_function" text,
  "notes" text
);
