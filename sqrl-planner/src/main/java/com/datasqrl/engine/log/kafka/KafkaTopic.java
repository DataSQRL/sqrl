package com.datasqrl.engine.log.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import lombok.Value;

import java.util.Optional;

@Value
public class KafkaTopic implements Log {

  String topicName;
  Name logName;
  TableConfig logConfig;
  Optional<TableSchema> tableSchema;

  @Override
  public TableSource getSource() {
    return logConfig.initializeSource(logName.toNamePath(), tableSchema.get());
  }

  @Override
  public TableSink getSink() {
    return logConfig.initializeSink(logName.toNamePath(), tableSchema);
  }
}
