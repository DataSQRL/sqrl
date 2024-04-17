package com.datasqrl.engine.log.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.io.tables.TableSource;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

@AllArgsConstructor
@Getter
public class KafkaTopic implements Log {

  String topicName;
  Name logName;
  TableConfig logConfig;
  Optional<TableSchema> tableSchema;

  @Override
  public TableSource getSource() {
    return TableSource.create(logConfig, logName.toNamePath(), tableSchema.get());
  }

  @Override
  public TableSink getSink() {
    return TableSinkImpl.create(logConfig, logName.toNamePath(), tableSchema);
  }
}
