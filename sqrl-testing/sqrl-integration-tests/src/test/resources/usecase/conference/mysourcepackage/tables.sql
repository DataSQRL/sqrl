CREATE TABLE email_templates (
    id STRING NOT NULL,
    last_updated TIMESTAMP(3) NOT NULL,
    title STRING NOT NULL,
    fromEmail STRING NOT NULL,
    textBody STRING NOT NULL,
    toEmail STRING NOT NULL
) WITH (
    'connector' = 'filesystem',
    'path' = '/data/emailtemplates.json',
    'format' = 'flexible-json',
    'source.monitor-interval' = '1s'
);

CREATE TABLE Events (
    url STRING NOT NULL,
    `date` STRING NOT NULL,
    `time` STRING NOT NULL,
    title STRING NOT NULL,
    abstract STRING NOT NULL,
    location STRING NOT NULL,
    speakers ARRAY<ROW(name STRING NOT NULL, title STRING, company STRING NOT NULL)>,
    last_updated TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
    _sourcetime AS PROCTIME(),
    PRIMARY KEY (url) NOT ENFORCED,
     WATERMARK FOR `last_updated` AS (`last_updated` - INTERVAL '0.001' SECOND)
) WITH (
    'connector' = 'filesystem',
    'path' = '/data/events.json',
    'format' = 'flexible-json',
    'source.monitor-interval' = '1s'
);
