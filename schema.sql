CREATE TABLE IF NOT EXISTS raw_events (
  arrival_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  payload jsonb NOT NULL,
  processed boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS raw_pending ON raw_events(arrival_id) WHERE NOT processed;
CREATE TABLE IF NOT EXISTS events (
  event_id text PRIMARY KEY,
  sensor_id text NOT NULL,
  event_time timestamptz NOT NULL,
  value bigint NOT NULL,
  arrival_id bigint NOT NULL UNIQUE REFERENCES raw_events(arrival_id),
  is_late boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
  arrival_id bigint PRIMARY KEY REFERENCES raw_events(arrival_id),
  reason text NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_state (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  max_event_time timestamptz
);
INSERT INTO stream_state(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS minute_metrics (
  bucket timestamptz NOT NULL,
  sensor_id text NOT NULL,
  event_count bigint NOT NULL,
  value_sum bigint NOT NULL,
  PRIMARY KEY(bucket, sensor_id)
);
CREATE TABLE IF NOT EXISTS daily_metrics (
  day date NOT NULL,
  sensor_id text NOT NULL,
  event_count bigint NOT NULL,
  value_sum bigint NOT NULL,
  computed_at timestamptz NOT NULL,
  PRIMARY KEY(day, sensor_id)
);
