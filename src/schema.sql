CREATE TABLE IF NOT EXISTS cpd_incidents (
    id INTEGER PRIMARY KEY, date_time TIMESTAMP, type TEXT, subtype TEXT,
    location TEXT, description TEXT, last_updated TIMESTAMP,
    year INT, month INT, day INT, hour INT
);
CREATE INDEX IF NOT EXISTS idx_cpd_date ON cpd_incidents (date_time);
-- Supabase exposes public tables through its REST API: lock it, our pipeline/dashboard use direct Postgres.
ALTER TABLE cpd_incidents ENABLE ROW LEVEL SECURITY;
