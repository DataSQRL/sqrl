package com.datasqrl.engine.log.kafka;

import static com.datasqrl.engine.log.kafka.KafkaLogEngineFactory.ENGINE_NAME;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.io.tables.TableSource;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

@AllArgsConstructor
@Getter
class KafkaTopic implements Log {

  String topicName;
  Name logName;
  TableConfig logConfig;
  Optional<TableSchema> tableSchema;
  IConnectorFactoryContext connectorContext;
  RelDataType schema;

  @Override
  public TableSource getSource() {
    return TableSource.create(logConfig, NamePath.of(ENGINE_NAME), tableSchema.get());
  }

  @Override
  public TableSink getSink() {
    return TableSinkImpl.create(logConfig, NamePath.of(ENGINE_NAME), tableSchema);
  }

}
