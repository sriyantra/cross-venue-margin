-- The engine's event log. Rows are only ever added.
CREATE TABLE events (
    -- Order of emission. One writer inserts in order, so a higher seq is a
    -- later event. Gaps are possible and mean nothing.
    seq         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  BIGINT      NOT NULL,
    kind        TEXT        NOT NULL,
    -- Engine time of the event, in milliseconds since the Unix epoch.
    at_ms       BIGINT      NOT NULL,
    -- The whole event. Amounts are JSON strings, so no digit is lost.
    payload     JSONB       NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX events_account_seq ON events (account_id, seq);

CREATE FUNCTION events_are_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'the events table is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_no_change
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_are_append_only();

CREATE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_are_append_only();
