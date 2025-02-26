package com.datasqrl.engine.log.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kafka.common.internals.Topic;

@AllArgsConstructor
public class KafkaLogFactory implements LogFactory {

  ConnectorFactory connectorFactory;

  @Override
  public Log create(
      String logId,
      Name logName,
      RelDataType schema,
      List<String> primaryKey,
      Timestamp timestamp) {

    Preconditions.checkArgument(Topic.isValid(logId), "Not a valid topic name: %s", logId);
    IConnectorFactoryContext connectorContext =
        createSinkContext(
            logName, logId, timestamp.getName(), timestamp.getType().name(), primaryKey);
    TableConfig logConfig = connectorFactory.createSourceAndSink(connectorContext);
    Optional<TableSchema> tblSchema = Optional.of(new RelDataTypeTableSchema(schema));
    return new KafkaTopic(logId, logName, logConfig, tblSchema, connectorContext, schema);
  }

  @Override
  public String getEngineName() {
    return KafkaLogEngineFactory.ENGINE_NAME;
  }

  private IConnectorFactoryContext createSinkContext(
      Name name,
      String topicName,
      String timestampName,
      String timestampType,
      List<String> primaryKey) {
    Map<String, Object> context = new HashMap<>();
    context.put("topic", topicName);
    context.put("timestamp-name", timestampName);
    context.put("timestamp-type", timestampType);
    context.put("primary-key", primaryKey);
    return new ConnectorFactoryContext(name, context);
  }
}
