package ai.datasqrl.physical;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.metadata.FileMetadataStore;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine.Builder;
import ai.datasqrl.execute.flink.environment.LocalFlinkStreamEngineImpl;
import ai.datasqrl.execute.flink.ingest.DataStreamProvider;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess.Error;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.io.sources.SourceRecord.Named;
import ai.datasqrl.io.sources.SourceRecord.Raw;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.database.CreateTableBuilder;
import ai.datasqrl.physical.database.ddl.CreateTableDDL;
import ai.datasqrl.physical.database.ddl.DropTableDDL;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.FlinkPipelineGenerator;
import ai.datasqrl.plan.LogicalPlan;
import ai.datasqrl.plan.RelQuery;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.type.schema.SchemaAdjustmentSettings;
import ai.datasqrl.sql.RelToSql;
import ai.datasqrl.validate.imports.ImportManager;
import ai.datasqrl.validate.imports.ImportManager.SourceTableImport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

@AllArgsConstructor
public class Physicalizer {
  ImportManager importManager;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;

  public ExecutionPlan plan(LogicalPlan plan) {
    CreateStreamJobResult result = createStreamJobGraph(plan.getStreamQueries());

    List<SqlDDLStatement> databaseDDL = createDatabaseDDL(result.createdTables, plan.getDatabaseQueries());

    return new ExecutionPlan(databaseDDL, result.streamQueries, plan.getSchema());
  }


  private List<SqlDDLStatement> createDatabaseDDL(
      List<TableDescriptor> createdTables, List<RelQuery> databaseQueries) {
    //1. All streamQueries get created into tables
    List<SqlDDLStatement> statements = new ArrayList<>();
    for (TableDescriptor sink : createdTables) {
      DropTableDDL dropTableDDL = new DropTableDDL(sink.getOptions().get("table-name"));
      statements.add(dropTableDDL);

      CreateTableDDL createTableDDL = createStreamTables(sink);
      statements.add(createTableDDL);
    }

    for (RelQuery view : databaseQueries) {
      //todo: create views
    }

    return statements;
  }

  private CreateTableDDL createStreamTables(TableDescriptor table) {
    Schema schema = table.getSchema().get();
    List<String> pk;

    if (schema.getPrimaryKey().isPresent()) {
      UnresolvedPrimaryKey key = schema.getPrimaryKey().get();
      pk = key.getColumnNames();
    } else {
      throw new RuntimeException("Unknown primary key");
    }

    List<String> columns = new ArrayList<>();
    for (UnresolvedColumn col : schema.getColumns()) {
      String column = addColumn((UnresolvedPhysicalColumn) col);
      if (column != null) {
        columns.add(column);
      }
    }

    return new CreateTableDDL(table.getOptions().get("table-name"), columns, pk);
  }

  public String addColumn(UnresolvedPhysicalColumn column) {
    if (column.getDataType() instanceof CollectionDataType) {

      return null;
    }

    AtomicDataType type = (AtomicDataType)column.getDataType();

    return addColumn(column.getName().toString(), getSQLType(type), !type.getLogicalType().isNullable());
  }

  private String addColumn(String name, String sqlType, boolean isNonNull) {
    StringBuilder sql = new StringBuilder();
//        name = sqlName(name);
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (isNonNull) sql.append("NOT NULL");
    return sql.toString();
//        if (isPrimaryKey) primaryKeys.add(name);
  }


  private String getSQLType(AtomicDataType type) {
    switch (type.getLogicalType().getTypeRoot()) {
      case BOOLEAN:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
        return "BIGINT";
      case CHAR:
      case VARCHAR:
        return "VARCHAR";
      case FLOAT:
      case DOUBLE:
        return "FLOAT";
      case DATE:
        return "DATE";
      case TIME_WITHOUT_TIME_ZONE:
        return "TIME";
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return "TIMESTAMP";
      case TIMESTAMP_WITH_TIME_ZONE:
        return "TIMESTAMPTZ";
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TIMESTAMPTZ";
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
      case NULL:
      case SYMBOL:
      case UNRESOLVED:
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }

  private CreateStreamJobResult createStreamJobGraph(List<RelQuery> streamQueries) {
    List<TableDescriptor> createdTables = new ArrayList<>();
    Builder streamBuilder = ((LocalFlinkStreamEngineImpl)streamEngine).createStream();
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)
        StreamTableEnvironment.create(streamBuilder.getEnvironment());

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    for (RelQuery sink : streamQueries) {
      registerStream(sink, tEnv, streamBuilder);

      String sql = RelToSql.convertToSql(sink.getRelNode().getInput(0)).replaceAll("\"", "`");
      org.apache.flink.table.api.Table tbl = tEnv.sqlQuery(sql);

      String name = sink.getTable().name.getCanonical() + "_sink";

      Schema schema = FlinkPipelineGenerator.addPrimaryKey(tbl.getSchema().toSchema(), sink.getTable());

      TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
          .schema(schema)
          .option("url", "jdbc:postgresql://localhost/henneberger")
          .option("table-name", sink.getTable().name.getCanonical())
          .build();

      //Create sink
      tEnv.createTable(name, descriptor);
      createdTables.add(descriptor);

      stmtSet.addInsert(name, tbl);
    }

    return new CreateStreamJobResult(stmtSet, createdTables);
  }

  public void registerStream(RelQuery sink,
      StreamTableEnvironmentImpl tEnv,
      Builder streamBuilder) {
    ErrorCollector errors = ErrorCollector.root();

    final OutputTag<Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR") {
    };
    FlinkTableConverter tbConverter = new FlinkTableConverter();

    sink.getRelNode().accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        String streamName = scan.getTable().getQualifiedName().get(0);
        if ((List.of(tEnv.listTables()).contains(streamName))) {
          return super.visit(scan);
        }
        NamePath path = NamePath.parse(streamName);
        //TODO: resolve imports
        SourceTableImport sourceTable = importManager.resolveTable2(Name.system("ecommerce-data"), path.get(0),
            Optional.empty(), errors);

        DataStream<Raw> stream = new DataStreamProvider().getDataStream(sourceTable.getTable(),
            streamBuilder);
        Pair<Schema, TypeInformation> tableSchema = tbConverter.tableSchemaConversion(
            sourceTable.getSourceSchema());
        SingleOutputStreamOperator<Named> validate = stream.process(
            new SchemaValidationProcess(schemaErrorTag, sourceTable.getSourceSchema(),
                SchemaAdjustmentSettings.DEFAULT,
                sourceTable.getTable().getDataset().getDigest()));

        SingleOutputStreamOperator<Row> rows = validate.map(
            tbConverter.getRowMapper(sourceTable.getSourceSchema()),
            tableSchema.getRight());

        tEnv.registerDataStream(streamName, rows);

        return super.visit(scan);
      }
    });
  }

  @Value
  class CreateStreamJobResult {
    StreamStatementSet streamQueries;
    List<TableDescriptor> createdTables;
  }
}
