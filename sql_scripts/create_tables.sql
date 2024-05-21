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

CREATE TABLE api_football_first_load.season_prep
(
  league_id int4,
  seasons text[],
  load_dttm timestamp,
  source_id text,
  PRIMARY KEY (league_id)
);

CREATE VIEW api_football_first_load.season_v AS
WITH cte AS (SELECT league_id, unnest(seasons)::jsonb AS season FROM api_football_first_load.season_prep)
SELECT
	row_number() over() AS season_id,
	league_id,
	(season -> 'year')::text AS YEAR,
	to_date(replace((season -> 'start')::text, '"', ''), 'yyyy-MM-dd') AS start,
	to_date(replace((season -> 'end')::text, '"', ''), 'yyyy-MM-dd') AS END,
	(season -> 'current')::text:: Boolean AS current
FROM cte;




