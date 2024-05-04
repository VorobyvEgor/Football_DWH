CREATE SCHEMA api_football_first_load;

CREATE TABLE api_football_first_load.countries
(
  name text,
  code varchar(2),
  country_flag_url text,
  load_dttm timestamp,
  source_id text,
  PRIMARY KEY (name)
);

CREATE TABLE api_football_first_load.leagues
(
  league_id int4,
  name text,
  type text,
  logo text,
  country_name text,
  load_dttm timestamp,
  source_id text,
  PRIMARY KEY (league_id)
);



