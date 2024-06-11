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
WITH cte AS (SELECT league_id, unnest(seasons)::jsonb AS season, load_dttm, source_id FROM api_football_first_load.season_prep)
SELECT
	md5(concat(league_id::text, (season -> 'year')::text))::uuid AS season_id,
	(season -> 'year')::text AS year,
	to_date(replace((season -> 'start')::text, '"', ''), 'yyyy-MM-dd') AS start,
	to_date(replace((season -> 'end')::text, '"', ''), 'yyyy-MM-dd') AS end,
	(season -> 'current')::text:: Boolean AS current,
	league_id,
	load_dttm,
	source_id
FROM cte;

CREATE VIEW api_football_first_load.coverage_v AS
WITH season AS
(SELECT
	league_id,
	(season -> 'year')::text AS year,
	(season -> 'coverage')::jsonb AS coverage,
	load_dttm,
	source_id
FROM (
	SELECT
		league_id,
		unnest(seasons)::jsonb AS season,
		load_dttm,
		source_id
	FROM api_football_first_load.season_prep))
SELECT
	row_number() over() AS coverage_id,
	(coverage -> 'standings')::boolean AS standings,
	(coverage -> 'players')::boolean AS players,
	(coverage -> 'top_scorers')::boolean AS top_scorers,
	(coverage -> 'top_assists')::boolean AS top_assists,
	(coverage -> 'top_cards')::boolean AS top_cards,
	(coverage -> 'injuries')::boolean AS injuries,
	(coverage -> 'predictions')::boolean AS predictions,
	(coverage -> 'odds')::boolean AS odds,
	md5(concat(league_id::text, year))::uuid AS season_id,
	load_dttm,
	source_id
FROM season;

CREATE VIEW api_football_first_load.fixture_v AS
WITH coverage AS
(SELECT
	league_id,
	(season -> 'year')::text AS year,
	(season -> 'coverage')::jsonb AS coverage,
	load_dttm,
	source_id
FROM (
	SELECT
		league_id,
		unnest(seasons)::jsonb AS season,
		load_dttm,
		source_id
	FROM api_football_first_load.season_prep)),
fixtures AS
(SELECT
	league_id,
	year,
	(coverage -> 'fixtures') AS fixtures,
	md5(concat(league_id::text, year))::uuid AS season_id,
	load_dttm,
	source_id
FROM coverage
)
SELECT
	row_number() over() AS fixtures_id,
	(fixtures -> 'events')::boolean AS events,
	(fixtures -> 'lineups')::boolean AS lineups,
	(fixtures -> 'statistics_fixtures')::boolean AS statistics_fixtures,
	(fixtures -> 'statistics_players')::boolean AS statistics_players,
	md5(concat(league_id::text, year))::uuid AS season_id,
	load_dttm,
	source_id
FROM fixtures;

CREATE TABLE api_football_first_load.team_info
(
  season_year int4,
  league_id int4,
  team_id int4,
  name text,
  code text,
  country text,
  founded int4,
  national boolean,
  logo text,
  load_dttm timestamp,
  source_id text,
  PRIMARY KEY (season_year, league_id, team_id)
);

CREATE TABLE api_football_first_load.team_venue
(
  venue_id int4,
  team_id int4,
  name text,
  address text,
  city text,
  capacity int4,
  surface text,
  image text,
  load_dttm timestamp,
  source_id text,
  PRIMARY KEY (team_id, venue_id)
);
