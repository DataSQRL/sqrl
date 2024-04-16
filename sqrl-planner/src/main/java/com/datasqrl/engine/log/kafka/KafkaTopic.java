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
    return initializeSource(logConfig, logName.toNamePath(), tableSchema.get());
  }

  public TableSource initializeSource(TableConfig tableConfig, NamePath basePath, TableSchema schema) {
//    getErrors().checkFatal(getBase().getType().isSource(), "Table is not a source: %s", name);
    Name tableName = tableConfig.getName();
    return new TableSource(tableConfig, basePath.concat(tableName), tableName, schema);
  }

  public TableSink initializeSink(TableConfig tableConfig, NamePath basePath, Optional<TableSchema> schema) {
//    getErrors().checkFatal(getBase().getType().isSink(), "Table is not a sink: %s", name);
    Name tableName = tableConfig.getName();
    return new TableSinkImpl(tableConfig, basePath.concat(tableName), tableName, schema);
  }
  @Override
  public TableSink getSink() {
    return initializeSink(logConfig, logName.toNamePath(), tableSchema);
  }
}
