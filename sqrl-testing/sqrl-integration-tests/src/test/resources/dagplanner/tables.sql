CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofmonth` AS 'com.datasqrl.time.EndOfMonth' LANGUAGE JAVA;

CREATE TEMPORARY TABLE `source_table` (
  id INT NOT NULL,
  data STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR `event_time` AS (`event_time` - INTERVAL '1' SECOND),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'datagen',
   'number-of-rows' = '20'
);
