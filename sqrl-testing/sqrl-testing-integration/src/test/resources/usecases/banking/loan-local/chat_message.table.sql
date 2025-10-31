CREATE TABLE ChatMessage (
    _uuid STRING NOT NULL METADATA FROM 'uuid',
    role STRING NOT NULL,
    content STRING NOT NULL,
    name STRING,
    context ROW(`customerid` INT) NOT NULL,
    event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
);
