package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.util.StreamUtil;
import com.google.inject.Inject;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

public class SnowflakeIcebergEngine extends AbstractJDBCEngine {

  @Inject
  public SnowflakeIcebergEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(SnowflakeIcebergEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfig(SnowflakeIcebergEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(SnowflakeIcebergEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Snowflake;
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {

    List<IcebergSerializableTable> sinks = StreamUtil.filterByClass(inputs,
            EngineSink.class)
        .map(c->new IcebergSerializableTable("namespace", c.getName(),
            convertRelDataTypeToIcebergSchema(c.getRowType())))
        .collect(Collectors.toList());

    JDBCPhysicalPlan plan1 = (JDBCPhysicalPlan)super.plan(plan, inputs, pipeline, framework, errorCollector);
    //todo end

    return new IcebergPlan(sinks, plan1.getDdl(), plan1.getQueries());
  }
  @Value
  public class IcebergSerializableTable {
    //todo make list
    String namespace;
    String name;
    List<IcebergSerializableColumn> columns;
  }

  @Value
  public class IcebergSerializableColumn {
    boolean optional;
    int index;
    String name;
    String typeName;
  }

  public List<IcebergSerializableColumn> convertRelDataTypeToIcebergSchema(RelDataType relDataType) {
    List<IcebergSerializableColumn> icebergFields = relDataType.getFieldList().stream()
        .map(field -> convertField(field))
        .collect(Collectors.toList());

    return icebergFields;
  }

  /**
   * Converts a Calcite field to an Iceberg field.
   * @param field The Calcite field.
   * @return The Iceberg field.
   */
  private IcebergSerializableColumn convertField(RelDataTypeField field) {
    int index = field.getIndex() + 1; // Iceberg fields are 1-indexed
    String name = field.getName();
    boolean optional = field.getType().isNullable();
    String typeName = getIcebergTypeName(field.getType().getSqlTypeName());

    return new IcebergSerializableColumn(optional, index, name, typeName);
  }

  /**
   *     BOOLEAN(Boolean.class),
   *     INTEGER(Integer.class),
   *     LONG(Long.class),
   *     FLOAT(Float.class),
   *     DOUBLE(Double.class),
   *     DATE(Integer.class),
   *     TIME(Long.class),
   *     TIMESTAMP(Long.class),
   *     STRING(CharSequence.class),
   *     UUID(java.util.UUID.class),
   *     FIXED(ByteBuffer.class),
   *     BINARY(ByteBuffer.class),
   *     DECIMAL(BigDecimal.class),
   *     STRUCT(StructLike.class),
   *     LIST(List.class),
   *     MAP(Map.class);
   *
   *     and struct type
   */
  private static String getIcebergTypeName(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
      case BIGINT:
        return "LongType";
      case INTEGER:
        return "IntegerType";
      case FLOAT:
        return "FloatType";
      case DOUBLE:
        return "DoubleType";
      case DATE:
        return "DateType";
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TimestampType";
      case VARCHAR:
        return "StringType";

      default:
        throw new IllegalArgumentException("Unsupported Calcite SQL type: " + sqlTypeName);
    }
  }
}
