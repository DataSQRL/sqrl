package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.api.graphql.GraphqlSchemaBuilder;
import ai.dataeng.sqml.api.graphql.SqrlCodeRegistryBuilder;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration.Dialect;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.execution.SqlGenerator;
import ai.dataeng.sqml.execution.flink.ingest.DataStreamProvider;
import ai.dataeng.sqml.execution.flink.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.parser.RelToSql;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.SchemaUpdaterImpl;
import ai.dataeng.sqml.parser.SqlNodeToFieldMapper;
import ai.dataeng.sqml.parser.SqrlParser;
import ai.dataeng.sqml.parser.SqrlToSqlParser;
import ai.dataeng.sqml.parser.operator.C360Test;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.parser.operator.ImportResolver;
import ai.dataeng.sqml.parser.operator.ShadowingContainer;
import ai.dataeng.sqml.parser.sqrl.operations.DatasetImportOperation;
import ai.dataeng.sqml.parser.sqrl.operations.SqrlQueryOperation;
import ai.dataeng.sqml.planner.LogicalPlanDag;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.planner.nodes.LogicalPgSink;
import ai.dataeng.sqml.planner.nodes.LogicalSqrlSink;
import ai.dataeng.sqml.planner.nodes.StreamTableScan;
import ai.dataeng.sqml.schema.NamespaceImpl;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.ReservedName;
import ai.dataeng.sqml.tree.name.VersionedName;
import ai.dataeng.sqml.type.CalciteDelegatingField;
import ai.dataeng.sqml.type.schema.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import graphql.GraphQL;
import graphql.com.google.common.collect.Maps;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqrlRelBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ImportToNamespaceTest {
  NamespaceImpl namespace;
  ImportProcessor importProcessor;
  QueryProcessor queryProcessor;
  SqrlParser sqrlParser;
  Environment sqrlEnv = null;
  DatasetRegistry registry = null;

  ImportManager imports;

  FlinkTableConverter tbConverter;
  private CalciteSchema schema;
  private RelOptCluster cluster;
  private CalciteCatalogReader catalogReader;
  private JavaTypeFactoryImpl typeFactory;

  @BeforeEach
  public void setup() throws Exception {
    FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    SqrlSettings settings = ConfigurationTest.getDefaultSettings(true);
    sqrlEnv = Environment.create(settings);
    registry = sqrlEnv.getDatasetRegistry();

    sqrlParser = new SqrlParser();
    namespace = new NamespaceImpl();
    setCalcite();

    importProcessor = new ImportProcessor(namespace, new ImportResolver(new ImportManager(registry)));
    queryProcessor = new QueryProcessor(namespace, new SqrlToSqlParser(namespace), new SchemaUpdaterImpl(namespace), new SqlNodeToFieldMapper(namespace));

    imports = new ImportManager(registry);
    tbConverter = new FlinkTableConverter();
    registerDatasets();
//    when(importProcessor.process(any())).thenReturn(List.of(new DatasetImportOperation(createImport())));
//    when(queryProcessor.process(any())).thenReturn(List.of(new SqrlQueryOperation()));
  }

  private void registerDatasets() {
//    ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();

    String ds2Name = "ecommerce-data";
    FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
        .uri(C360Test.RETAIL_DATA_DIR.toAbsolutePath().toString())
        .name(ds2Name)
        .build();
    registry.addOrUpdateSource(fileConfig, ErrorCollector.root());
//    assertFalse(errors.isFatal());


    //Needs some time to wait for the flink pipeline to compile data
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @AfterEach
  public void close() {
    sqrlEnv.close();
    sqrlEnv = null;
    registry = null;
  }

  @Test
  public void testImport() {
    importProcessor.process(sqrlParser.parseStatement(("IMPORT ecommerce-data.Orders")));
    queryProcessor.process(sqrlParser.parseStatement(("o2 := SELECT _uuid as uid FROM Orders")));

    namespace.populate();
    LogicalPlanDag dag = namespace.getLogicalPlanDag();

    assignDevQueries(dag);

    Pair<List<LogicalFlinkSink>, List<LogicalPgSink>> flinkSinks = optimize(dag);

    Pair<StreamStatementSet, Map<ai.dataeng.sqml.parser.Table, TableDescriptor>> result =
        createFlinkPipeline(flinkSinks.getKey());

    SqlGenerator sqlGenerator = new SqlGenerator(result.getRight());
    List<String> db = sqlGenerator.generate();

    JDBCConnectionProvider config = new JDBCConfiguration.Database(
        "jdbc:postgresql://localhost/henneberger",
        null, null, null, Dialect.POSTGRES, "henneberger"
    );
    executeDml(config, db);

    TableResult rslt = result.getLeft().execute();
    rslt.print();

    System.out.println(dag);

    GraphQL graphql = graphql(dag, flinkSinks, result.getRight());
    execute(graphql, dag);
  }

  private void executeDml(JDBCConnectionProvider configuration, List<String> dmlQueries) {
      String dmls = dmlQueries.stream().collect(Collectors.joining("\n"));
      try (Connection conn = configuration.getConnection(); Statement stmt = conn.createStatement()) {
          stmt.executeUpdate(dmls);
      } catch (SQLException e) {
          throw new RuntimeException("Could not execute SQL query",e);
      } catch (ClassNotFoundException e) {
          throw new RuntimeException("Could not load database driver",e);
      }
  }

  private Pair<StreamStatementSet, Map<ai.dataeng.sqml.parser.Table, TableDescriptor>> createFlinkPipeline(List<LogicalFlinkSink> sinks) {
    FlinkTableConverter tbConverter = new FlinkTableConverter();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)StreamTableEnvironment.create(env);
    StreamPlanner streamPlanner = (StreamPlanner) tEnv.getPlanner();
    FlinkRelBuilder builder = streamPlanner.getRelBuilder();
    builder.getCluster();
    final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR"){};

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    Map<ai.dataeng.sqml.parser.Table, TableDescriptor> ddl = new HashMap<>();

    for (LogicalFlinkSink sink : sinks) {
      //Register data streams
      sink.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(TableScan scan) {
          if (scan instanceof StreamTableScan) {
            //1. construct to stream, register it with tenv
            ImportManager.SourceTableImport imp = ((StreamTableScan)scan).getTableImport();
            Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(imp.getSourceSchema());

            DataStream<SourceRecord.Raw> stream = new DataStreamProvider().getDataStream(imp.getTable(),env);
            SingleOutputStreamOperator<SourceRecord.Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, imp.getSourceSchema(),
                SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest()));

            SingleOutputStreamOperator<Row> rows = validate.map(tbConverter.getRowMapper(imp.getSourceSchema()),
                ordersSchema.getRight());

            List<String> tbls = List.of(tEnv.listTables());
            if (!(tbls.contains("orders"))) {
              String fields = ordersSchema.getKey().getColumns().stream()
                  .map(c->(UnresolvedPhysicalColumn) c)
                  .filter(c->c.getDataType() instanceof AtomicDataType)
                  .filter(c->!c.getName().equals(ReservedName.INGEST_TIME.getCanonical()) &&!c.getName().equals(ReservedName.SOURCE_TIME.getCanonical()))
                  .map(c->String.format("`"+c.getName()+"` AS `%s`", c.getName() + VersionedName.ID_DELIMITER + "0"))
                  .collect(Collectors.joining(", "));

              tEnv.registerDataStream("orders_stream", rows);
//              PlannerQueryOperation op = (PlannerQueryOperation)tEnv.getParser().parse(" + orders_stream).get(0);

              tEnv.sqlUpdate("CREATE TEMPORARY VIEW orders AS SELECT "+fields+" FROM orders_stream");
            }
          }
          return super.visit(scan);
        }

      });

      Table tbl = tEnv.sqlQuery(RelToSql.convertToSql(sink.getInput(0)).replaceAll("\"", "`"));

      String name = sink.getQueryTable().getName().getCanonical() + "_Sink";
      sink.setPhysicalName(name);
      List<String> tbls = List.of(tEnv.listTables());
      if (!(tbls.contains(name))) {
        TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
            .schema(tbl.getSchema().toSchema())
            .option("url", "jdbc:postgresql://localhost/henneberger")
            .option("table-name", name)
            .build();
        tEnv.createTable(name, descriptor);
        ddl.put(sink.getQueryTable(), descriptor);
      }

      //add dml to flink sink.

      stmtSet.addInsert(name, tbl);
    }


    return Pair.of(stmtSet, ddl);
  }

  /**
   * Executes a maximal query (w/ no loops) to the dag
   */
  private void execute(GraphQL graphql, LogicalPlanDag dag) {
    System.out.println(graphql.execute("query { orders { data { id } } }"));
  }

  /**
   * Builds a graphql schema & code registry. Code registry looks at sinks to determine
   * names and tables.
   */
  private GraphQL graphql(LogicalPlanDag dag,
      Pair<List<LogicalFlinkSink>, List<LogicalPgSink>> flinkSinks,
      Map<ai.dataeng.sqml.parser.Table, TableDescriptor> right) {
    Map<ai.dataeng.sqml.parser.Table, LogicalFlinkSink> sinks = Maps.uniqueIndex(flinkSinks.getLeft(), e->e.getQueryTable());
    GraphQLCodeRegistry codeRegistryBuilder = new SqrlCodeRegistryBuilder()
        .build(getPostgresClient(), sinks, right);

    GraphQLSchema graphQLSchema = GraphqlSchemaBuilder.newGraphqlSchema()
        .schema(dag.getSchema())
        .setCodeRegistryBuilder(codeRegistryBuilder)
        .build();

    System.out.println(new SchemaPrinter().print(graphQLSchema));

    GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema).build();

    return graphQL;
  }

  private SqlClientProvider getPostgresClient() {
    //TODO: this is hardcoded for now and needs to be integrated into configuration
    JDBCPool pool = JDBCPool.pool(
        Vertx.vertx(),
        new JDBCConnectOptions()
            .setJdbcUrl("jdbc:postgresql://localhost/henneberger"),
        new PoolOptions()
            .setMaxSize(1)
    );

    return () -> pool;
  }

  /**
   * Adds a sink operation to every non-shadowed table
   */
  private void assignDevQueries(LogicalPlanDag dag) {
    final Set<ai.dataeng.sqml.parser.Table> included = new HashSet<>();
    final Set<ai.dataeng.sqml.parser.Table> toInclude = new HashSet<>();

    dag.getSchema().visibleList().stream()
        .filter(t -> t instanceof ai.dataeng.sqml.parser.Table)
        .map(t -> (ai.dataeng.sqml.parser.Table)t)
        .forEach(t -> toInclude.add(t));

    while (!toInclude.isEmpty()) {
      ai.dataeng.sqml.parser.Table next = toInclude.iterator().next();
        assert !included.contains(next);
        included.add(next);
        toInclude.remove(next);
        //Find all non-hidden related tables and add those
        next.getFields().visibleStream().filter(f -> f instanceof Relationship && !f.name.isHidden())
                .map(f -> (Relationship)f)
                .forEach(r -> {
                    Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships: " + r.toTable.name);
                    if (!included.contains(r.toTable)) {
                        toInclude.add(r.toTable);
                    }
                });
    }

    for (ai.dataeng.sqml.parser.Table queryTable : included) {
      if (queryTable.getRelNode() == null )continue;
      assert queryTable.getRelNode()!=null;
      LogicalSqrlSink sink = new LogicalSqrlSink(queryTable.getRelNode().getCluster(), RelTraitSet.createEmpty(), queryTable.getRelNode(), queryTable);
      queryTable.setRelNode(sink);
    }
  }

  /**
   * Cuts the dag into two parts
   * - Flink pipeline nodes w/ postgres sinks
   * - Postgres table scans to materialized view sinks
   * @return
   */
  private Pair<List<LogicalFlinkSink>, List<LogicalPgSink>> optimize(LogicalPlanDag dag) {
    cut(dag);

    //Convert sink to Physical
    final Set<ai.dataeng.sqml.parser.Table> included = new HashSet<>();
    final Set<ai.dataeng.sqml.parser.Table> toInclude = new HashSet<>();
    List<LogicalFlinkSink> flinkSinks = new ArrayList<>();

    dag.getSchema().visibleStream().filter(t -> t instanceof ai.dataeng.sqml.parser.Table)
        .map(t -> (ai.dataeng.sqml.parser.Table)t).forEach(t -> toInclude.add(t));

    while (!toInclude.isEmpty()) {
      ai.dataeng.sqml.parser.Table next = toInclude.iterator().next();
      assert !included.contains(next);
      included.add(next);
      toInclude.remove(next);
      //Find all non-hidden related tables and add those
      next.getFields().visibleStream().filter(f -> f instanceof Relationship && !f.name.isHidden())
          .map(f -> (Relationship)f)
          .forEach(r -> {
            Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships: " + r.toTable.name);
            if (!included.contains(r.toTable)) {
              toInclude.add(r.toTable);
            }
          });
    }


    for (ai.dataeng.sqml.parser.Table queryTable : included) {
      if (queryTable.getRelNode() == null) continue;
      assert queryTable.getRelNode()!=null;
      if (queryTable.getRelNode() instanceof LogicalSqrlSink) {
        LogicalSqrlSink sink = (LogicalSqrlSink)queryTable.getRelNode();
        flinkSinks.add(new LogicalFlinkSink(sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getQueryTable()));
      }
    }

    return Pair.of(flinkSinks, List.of());
  }

  private void cut(LogicalPlanDag dag) {

  }

  private void applyOperation(DatasetImportOperation dsOperation, LogicalPlanDag dag) {
    dag.mergeSchema(dsOperation.getDag().getSchema());
  }

  private void applyOperation(SqrlQueryOperation op, LogicalPlanDag dag) {
    //new var
    ai.dataeng.sqml.parser.Table table = new ai.dataeng.sqml.parser.Table(1, Name.system("o2"), NamePath.of(Name.system("o2")), false);
    String query = "SELECT _uuid AS uid FROM orders";

    SqrlRelBuilder builder = new SqrlRelBuilder(null, this.cluster, this.catalogReader);

    ImportManager.SourceTableImport ordersImp = imports.importTable(Name.system("c360"),Name.system("orders"),ErrorCollector.root());
    builder.scanStream(ordersImp);
    RexNode call = builder.getRexBuilder().makeCall(SqlStdOperatorTable.AS, List.of(RexInputRef.of(0, builder.peek().getRowType()),
        builder.getRexBuilder().makeLiteral("uid")));
    builder.project(List.of(call));
    RelNode node = builder.build();
    table.setRelNode(node);

    table.addField(CalciteDelegatingField.of(0, node.getRowType()));

    dag.getSchema()
            .add(table);
  }

  private LogicalPlanDag createImport() throws Exception {
    ImportManager imports = new ImportManager(registry);

    ImportManager.SourceTableImport ordersImp = imports.importTable(Name.system("c360"),Name.system("orders"),ErrorCollector.root());
    FlinkTableConverter converter = new FlinkTableConverter();
    Pair<Schema, TypeInformation> schema = converter.tableSchemaConversion(ordersImp.getSourceSchema());
    System.out.println(schema.getLeft().getColumns().get(0).getName());;

    ai.dataeng.sqml.parser.Table orders = new ai.dataeng.sqml.parser.Table(0, Name.system("Orders"), NamePath.of(Name.system("Orders")), false);

    for (UnresolvedColumn column : schema.getLeft().getColumns()) {
      UnresolvedPhysicalColumn physicalColumn = (UnresolvedPhysicalColumn) column;
      AbstractDataType dataType = physicalColumn.getDataType();


    }

    //Least amount of code:
    // 1. Construct sqrl tables and add them to the dag directly
    // 2. The sqrl calcite table will resolve them

    SqrlRelBuilder builder = new SqrlRelBuilder(null, this.cluster, this.catalogReader);
    builder.scanStream(ordersImp);
    RelNode node = builder.build();

    RelDataType type = node.getRowType();

    //op
    orders.addField(CalciteDelegatingField.of(0, true, false, true, false, type));
    orders.addField(CalciteDelegatingField.of(1, true, false, false, false, type));
    orders.addField(CalciteDelegatingField.of(2, type));
    orders.addField(CalciteDelegatingField.of(3, type));
    orders.addField(CalciteDelegatingField.of(4, type));

    orders.setRelNode(node);

    ShadowingContainer shadowingContainer = new ShadowingContainer();
    shadowingContainer.add(orders);
    LogicalPlanDag dag = new LogicalPlanDag(shadowingContainer);
    return dag;
  }

  private RelDataType toType(AbstractDataType<?> dataType) {
    return null;
  }

  //Create the calcite cluster, schema

  // Configure and instantiate validator

  public void setCalcite() {
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    FlinkTypeFactory typeFactory = FlinkTypeFactory.INSTANCE();
//    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);
    this.catalogReader = catalogReader;

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    RelOptPlanner planner = new HepPlanner(HepProgram.builder().build());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    this.typeFactory = typeFactory;
    // Validate the initial AST
    this.cluster = cluster;
    this.schema = schema;

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)

  }



  //Temp
  private static class TempTable extends AbstractTable {
    private final RelDataType rowType;

    TempTable(RelDataType rowType) {
      this.rowType = rowType;
    }

    @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return rowType;
    }
  }

}
