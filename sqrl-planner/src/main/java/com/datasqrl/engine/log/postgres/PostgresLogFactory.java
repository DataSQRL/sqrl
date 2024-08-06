package com.datasqrl.engine.log.postgres;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor
public class PostgresLogFactory implements LogFactory {

  ConnectorFactory sourceConnectorFactory;
  ConnectorFactory sinkConnectorFactory;

  @Override
  public Log create(String logId, Name logName, RelDataType schema, List<String> primaryKey,
      Timestamp timestamp) {

    String tableName = logId;
    IConnectorFactoryContext connectorContext = createSinkContext(logName, tableName, timestamp.getName(),
        timestamp.getType().name(), primaryKey);

    TypeFactory typeFactory = TypeFactory.getTypeFactory();

    RelDataType patchedSchema = CalciteUtil.addField(
        schema,
        schema.getFieldCount(),
        timestamp.getName(),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
        typeFactory);

    TableConfig sourceConfig = sourceConnectorFactory.createSourceAndSink(connectorContext);
    TableConfig sinkConfig = sinkConnectorFactory.createSourceAndSink(connectorContext);
    RelDataTypeTableSchema tblSchema = new RelDataTypeTableSchema(patchedSchema);
    return new PostgresTable(tableName, logName, sourceConfig, sinkConfig, tblSchema, primaryKey, connectorContext, patchedSchema);
  }

  @Override
  public String getEngineName() {
    return PostgresLogEngineFactory.ENGINE_NAME;
  }

  private IConnectorFactoryContext createSinkContext(Name name, String tableName,
      String timestampName, String timestampType, List<String> primaryKey) {
    Map<String, Object> context = new HashMap<>();
    context.put("table-name", tableName);
    context.put("timestamp-name", timestampName);
    context.put("timestamp-type", timestampType);
    context.put("primary-key", primaryKey);
    return new ConnectorFactoryContext(name, context);
  }

}
