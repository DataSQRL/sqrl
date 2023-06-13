package com.datasqrl.kafka;

import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import lombok.Value;

@Value
public class KafkaTopic implements Log {

  String topicName;
  TableSource source;
  TableSink sink;

}
