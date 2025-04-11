CREATE TEMPORARY FUNCTION IF NOT EXISTS `tojson` AS 'com.datasqrl.json.ToJson' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `__DataSQRLUuidGenerator` AS 'com.datasqrl.secure.Uuid' LANGUAGE JAVA;

CREATE TABLE `orders$1` (
  `_source_time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `id` BIGINT NOT NULL,
  `customerid` BIGINT NOT NULL
  WATERMARK FOR `_source_time` AS (`_source_time` - INTERVAL '1' SECOND)
) WITH (
   'connector' = 'datagen',
   'number-of-rows' = '20'
);

CREATE TABLE `orders$2$1` (
  `_uuid` CHAR(36) CHARACTER SET `UTF-16LE` NOT NULL,
  `_ingest_time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `_source_time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `id` BIGINT NOT NULL,
  `customerid` BIGINT NOT NULL,
  `time` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `entries` ROW(`productid` INTEGER NOT NULL, `quantity` INTEGER NOT NULL, `unit_price` DOUBLE NOT NULL, `discount` DOUBLE) NOT NULL ARRAY NOT NULL,
  `json` RAW('com.datasqrl.types.json.FlinkJsonType', 'AEdvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uS3J5b1NlcmlhbGl6ZXJTbmFwc2hvdAAAAAIAH2NvbS5kYXRhc3FybC5qc29uLkZsaW5rSnNvblR5cGUAAATyxpo9cAAAAAIAH2NvbS5kYXRhc3FybC5qc29uLkZsaW5rSnNvblR5cGUBAAAAIQAfY29tLmRhdGFzcXJsLmpzb24uRmxpbmtKc29uVHlwZQEAAAAlAB9jb20uZGF0YXNxcmwuanNvbi5GbGlua0pzb25UeXBlAAAAAAApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAKwApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAB8AApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkAAAACrO0ABXNyAEJvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24uRXhlY3V0aW9uQ29uZmlnJFNlcmlhbGl6YWJsZVNlcmlhbGl6ZXJBDr5NoiCPtQIAAUwACnNlcmlhbGl6ZXJ0ACZMY29tL2Vzb3Rlcmljc29mdHdhcmUva3J5by9TZXJpYWxpemVyO3hwc3IAbW9yZy5hcGFjaGUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLnJ1bnRpbWUua3J5by5TZXJpYWxpemVycyRTcGVjaWZpY0luc3RhbmNlQ29sbGVjdGlvblNlcmlhbGl6ZXJGb3JBcnJheUxpc3QAAAAAAAAAAQIAAHhyAGFvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uU2VyaWFsaXplcnMkU3BlY2lmaWNJbnN0YW5jZUNvbGxlY3Rpb25TZXJpYWxpemVyAAAAAAAAAAECAAFMAAR0eXBldAARTGphdmEvbGFuZy9DbGFzczt4cHZyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAATyxpo9cAAAAAAAAATyxpo9cAAAAAA=')
) WITH (
  'properties.bootstrap.servers' = '${kafka.bootstrap}',
  'connector' = 'kafka',
  'format' = 'flexible-json',
  'topic' = 'orders',
  'properties.group.id' = 'datasqrl-orders'
);

CREATE VIEW `root$1`
AS
SELECT `_uuid`, `_ingest_time`, `_source_time`, `id`, `customerid`, `time`, `entries`, TOJSON('{"int": 1, "string": "str", "array": [0,1,2], "nested":{"key":"value"}}') AS `json`
FROM `orders$1`;

EXECUTE STATEMENT SET BEGIN
INSERT INTO `orders$2$1`
(SELECT *
FROM `root$1`)
;
END;