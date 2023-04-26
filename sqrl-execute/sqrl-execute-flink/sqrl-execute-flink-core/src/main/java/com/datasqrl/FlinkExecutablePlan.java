package com.datasqrl;

import com.datasqrl.config.BaseConnectorFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.model.StreamType;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.model.LogicalStreamMetaData;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
public class FlinkExecutablePlan {

  FlinkBase base;

  public interface FlinkExecutablePlanVisitor<R, C> {

    R visitPlan(FlinkExecutablePlan plan, C context);
  }

  public <R, C> R accept(FlinkExecutablePlanVisitor<R, C> visitor, C context) {
    return visitor.visitPlan(this, context);
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkBase {

    FlinkConfig config;
    @Builder.Default
    List<FlinkStatement> statements = new ArrayList<>();
    @Builder.Default
    List<FlinkFunction> functions = new ArrayList<>();
    List<FlinkTableDefinition> tableDefinitions;
    List<FlinkQuery> queries;
    List<FlinkSink> sinks;

    FlinkErrorSink errorSink;

    public <R, C> R accept(FlinkBaseVisitor<R, C> visitor, C context) {
      return visitor.visitBase(this, context);
    }
  }

  public interface FlinkBaseVisitor<R, C> {

    R visitBase(FlinkBase base, C context);
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkErrorSink {

    Class<? extends BaseConnectorFactory> connectorFactory;
    Class<? extends FormatFactory> formatFactory;
    TableConfig.Serialized tableConfig;
    String name;
    NamePath namePath;

    public <R, C> R accept(FlinkErrorSinkVisitor<R, C> visitor, C context) {
      return visitor.visitErrorSink(this, context);
    }
  }

  public interface FlinkErrorSinkVisitor<R, C> {

    R visitErrorSink(FlinkErrorSink errorSink, C context);
  }


  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  interface FlinkConfig {

    <R, C> R accept(FlinkConfigVisitor<R, C> visitor, C context);
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class DefaultFlinkConfig implements FlinkConfig {

    Map<String, String> streamExecutionEnvironmentConfig;
    Map<String, String> tableEnvironmentConfig;

    @Override
    public <R, C> R accept(FlinkConfigVisitor<R, C> visitor, C context) {
      return visitor.visitConfig(this, context);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public interface FlinkConfigVisitor<R, C> {

    R visitConfig(DefaultFlinkConfig config, C context);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public interface FlinkStatement {

    <R, C> R accept(FlinkStatementVisitor<R, C> visitor, C context);
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkJarStatement implements FlinkStatement {

    String path;

    @Override
    public <R, C> R accept(FlinkStatementVisitor<R, C> visitor, C context) {
      return visitor.visitJarStatement(this, context);
    }
  }

  public interface FlinkStatementVisitor<R, C> {

    R visitJarStatement(FlinkJarStatement statement, C context);
  }


  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public interface FlinkFunction {

    <R, C> R accept(FlinkFunctionVisitor<R, C> visitor, C context);
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkSqlFunction implements FlinkFunction {

    String functionSql;

    @Override
    public <R, C> R accept(FlinkFunctionVisitor<R, C> visitor, C context) {
      return visitor.visitFunction(this, context);
    }
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkJavaFunction implements FlinkFunction {

    //    private boolean isSystemFunction;
//    private boolean ifNotExists;
//    private String catalogName;
//    private String dbName;
    private String functionName;
    private String identifier;
//    private String lang;
//    private List<String> jarPaths;

    @Override
    public <R, C> R accept(FlinkFunctionVisitor<R, C> visitor, C context) {
      return visitor.visitFunction(this, context);
    }
  }

  public interface FlinkFunctionVisitor<R, C> {

    R visitFunction(FlinkJavaFunction fnc, C context);

    R visitFunction(FlinkSqlFunction flinkSqlFunction, C context);
  }


  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public interface FlinkTableDefinition {

    <R, C> R accept(FlinkTableDefinitionVisitor<R, C> visitor, C context);
  }

  public interface FlinkTableDefinitionVisitor<R, C> {

    R visitTableDefinition(FlinkSqlTableApiDefinition table, C context);

    R visitFactoryDefinition(FlinkFactoryDefinition table, C context);
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkSqlTableApiDefinition implements FlinkTableDefinition {

    String createSql;

    @Override
    public <R, C> R accept(FlinkTableDefinitionVisitor<R, C> visitor, C context) {
      return visitor.visitTableDefinition(this, context);
    }
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkFactoryDefinition implements FlinkTableDefinition {

    String name;
    Class<? extends BaseConnectorFactory> connectorFactory;
    Class<? extends FormatFactory> formatFactory;
    TableConfig.Serialized tableConfig;
    Class<? extends TableSchemaFactory> schemaFactory;
    String schemaDefinition;
    TypeInformation typeInformation;
    SerializableSchema schema;

    @Override
    public <R, C> R accept(FlinkTableDefinitionVisitor<R, C> visitor, C context) {
      return visitor.visitFactoryDefinition(this, context);
    }
  }


  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public interface FlinkQuery {

    <R, C> R accept(FlinkQueryVisitor<R, C> visitor, C context);
  }

  public interface FlinkQueryVisitor<R, C> {

    R visitQuery(FlinkSqlQuery query, C context);

    R visitQuery(FlinkStreamQuery query, C context);

  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkSqlQuery implements FlinkQuery {

    String name;
    String query;

    @Override
    public <R, C> R accept(FlinkQueryVisitor<R, C> visitor, C context) {
      return visitor.visitQuery(this, context);
    }
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkStreamQuery implements FlinkQuery {

    String name;
    StreamType stateChangeType;
    LogicalStreamMetaData meta;
    boolean unmodifiedChangelog;
    String fromTable;
    TypeInformation typeInformation;
    SerializableSchema schema;

    @Override
    public <R, C> R accept(FlinkQueryVisitor<R, C> visitor, C context) {
      return visitor.visitQuery(this, context);
    }
  }

  public interface FlinkSinkVisitor<R, C> {

    R visitSink(FlinkSqlSink table, C context);

  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public interface FlinkSink {

    <R, C> R accept(FlinkSinkVisitor<R, C> visitor, C context);

  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class FlinkSqlSink implements FlinkSink {

    String source;
    String target;

    @Override
    public <R, C> R accept(FlinkSinkVisitor<R, C> visitor, C context) {
      return visitor.visitSink(this, context);
    }
  }

  @Value
  public static class DataStreamResult {

    DataStream dataStream;
    List<SideOutputDataStream> errorSideChannels;
  }
}