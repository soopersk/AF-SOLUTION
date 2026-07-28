CREATE TABLE IF NOT EXISTS system_properties
(
    property_key   VARCHAR(255),
    property_value TEXT,
    PRIMARY KEY (property_key)
);

CREATE TABLE IF NOT EXISTS post_filter_control_dag_map
(
    team             VARCHAR(100) NOT NULL,
    filter_name      VARCHAR(100) NOT NULL,
    filter_condition TEXT NOT NULL,
    dag_id           VARCHAR(150) NOT NULL,
    enabled          BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (team, filter_name)
);

CREATE TABLE IF NOT EXISTS event
(
    event_id VARCHAR(36),
    json     JSONB,
    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS event_context_index ON event USING HASH ((json->>'contextId'));
CREATE INDEX IF NOT EXISTS event_additional_data_index ON event USING GIN ((json->'additionalData'));

CREATE TABLE IF NOT EXISTS context
(
    context_id VARCHAR(36),
    json       JSONB,
    PRIMARY KEY (context_id)
);

CREATE INDEX IF NOT EXISTS context_index ON context USING HASH (context_id);

CREATE INDEX IF NOT EXISTS context_data_index ON context USING GIN ((json->'data'));

ALTER TABLE system_properties ALTER COLUMN property_value TYPE TEXT;

INSERT INTO system_properties VALUES ('version', '0') ON CONFLICT (property_key) DO UPDATE SET property_value = EXCLUDED.property_value;    