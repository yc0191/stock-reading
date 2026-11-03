CREATE TABLE failed_event (
    id                BIGSERIAL PRIMARY KEY,
    topic             TEXT        NOT NULL,
    partition_no      INT         NOT NULL,
    record_offset     BIGINT      NOT NULL,
    record_key        TEXT,
    payload           TEXT,
    exception_class   TEXT        NOT NULL,
    exception_message TEXT,
    attempts          INT         NOT NULL,
    failed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, partition_no, record_offset)
);
