package com.datasqrl.engine.log.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.cmd.EngineKeys;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.log.LogEngine.Timestamp;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kafka.common.internals.Topic;

@AllArgsConstructor
public class KafkaLogFactory implements LogFactory {
  public static final char[] REPLACE_CHARS = {'$'};
  public static final char REPLACE_WITH = '-';

  ConnectorFactory connectorFactory;

  @Override
  public Log create(String logId, RelDataTypeField schema, List<String> primaryKey,
      Timestamp timestamp) {

    String topicName = sanitizeName(logId);
    Preconditions.checkArgument(Topic.isValid(topicName), "Not a valid topic name: %s", topicName);
    Name logName = Name.system(schema.getName());
    IConnectorFactoryContext connectorContext = createSinkContext(logName, topicName, timestamp.getName(),
        timestamp.getType().name(), primaryKey);
    TableConfig logConfig = connectorFactory
        .createSourceAndSink(connectorContext);
    Optional<TableSchema> tblSchema = Optional.of(new RelDataTypeTableSchema(schema.getType()));
    return new KafkaTopic(topicName, logName, logConfig, tblSchema, connectorContext);
  }

  static String sanitizeName(String logId) {
    String sanitizedName = logId;
    for (char invalidChar : REPLACE_CHARS) {
      sanitizedName = sanitizedName.replace(invalidChar, REPLACE_WITH);
    }
    return sanitizedName;
  }

  private IConnectorFactoryContext createSinkContext(Name name, String topicName,
      String timestampName, String timestampType, List<String> primaryKey) {
    Map<String, Object> context = new HashMap<>();
    context.put("topic", topicName);
    context.put("timestamp-name", timestampName);
    context.put("timestamp-type", timestampType);
    context.put("primary-key", primaryKey);
    return new ConnectorFactoryContext(name, context);
  }
}
