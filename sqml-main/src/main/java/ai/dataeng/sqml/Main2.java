package ai.dataeng.sqml;

import ai.dataeng.execution.DefaultDataFetcher;
import ai.dataeng.execution.connection.JdbcPool;
import ai.dataeng.execution.criteria.EqualsCriteria;
import ai.dataeng.execution.page.NoPage;
import ai.dataeng.execution.page.SystemPageProvider;
import ai.dataeng.execution.table.H2Table;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.UUIDColumn;
import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.operator.AggregateOperator;
import ai.dataeng.sqml.planner.operator.FilterOperator;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.operator.QueryAnalyzer;
import ai.dataeng.sqml.catalog.persistence.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.catalog.persistence.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.importer.ImportManager;
import ai.dataeng.sqml.importer.ImportSchema;
import ai.dataeng.sqml.execution.flink.environment.DefaultEnvironmentFactory;
import ai.dataeng.sqml.execution.flink.environment.EnvironmentFactory;
import ai.dataeng.sqml.execution.flink.ingest.DataSourceRegistry;
import ai.dataeng.sqml.execution.flink.ingest.DatasetRegistration;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.execution.flink.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.execution.flink.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.execution.flink.ingest.schema.external.SchemaExport;
import ai.dataeng.sqml.execution.flink.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceDataset;
import ai.dataeng.sqml.execution.flink.ingest.stats.SchemaGenerator;
import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.planner.optimize.LogicalPlanOptimizer;
import ai.dataeng.sqml.planner.optimize.SimpleOptimizer;
import ai.dataeng.sqml.execution.flink.process.FlinkConfiguration;
import ai.dataeng.sqml.execution.flink.process.FlinkGenerator;
import ai.dataeng.sqml.execution.sql.SQLConfiguration;
import ai.dataeng.sqml.execution.sql.SQLGenerator;
import ai.dataeng.sqml.execution.sql.util.DatabaseUtil;
import ai.dataeng.sqml.planner.operator.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.planner.operator.relation.RowExpression;
import ai.dataeng.sqml.type.basic.BasicTypeManager;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.importer.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.dataloader.DataLoaderRegistry;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Main2 {

    public static final Path RETAIL_DIR = Path.of(System.getProperty("user.dir")).resolve("sqml-examples").resolve("retail");
    public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
    public static final String RETAIL_DATASET = "ecommerce";
    public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
    public static final String RETAIL_SCRIPT_NAME = "c360";
    public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
    public static final String SQML_SCRIPT_EXTENSION = ".sqml";
    public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");

    public static final String[] RETAIL_TABLE_NAMES = { "customer", "orders", "product"};

    public static final Path outputBase = Path.of("tmp","datasource");
    public static final Path dbPath = Path.of("tmp","output");

    private static final EnvironmentFactory envProvider = new DefaultEnvironmentFactory();

    static final String JDBC_URL = "jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false";
    private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(JDBC_URL)
            .withDriverName("org.h2.Driver")
            .build();

    private static final FlinkConfiguration flinkConfig = new FlinkConfiguration(jdbcOptions);
    private static final SQLConfiguration sqlConfig = new SQLConfiguration(SQLConfiguration.Dialect.H2,jdbcOptions);

    private static final Name toName(String name) {
        return Name.of(name,NameCanonicalizer.LOWERCASE_ENGLISH);
    }

    public static void main(String[] args) throws Exception {
        BasicTypeManager.detectType("test");

        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
        DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
        DirectoryDataset dd = new DirectoryDataset(DatasetRegistration.of(RETAIL_DATASET), RETAIL_DATA_DIR);
        ddRegistry.addDataset(dd);

        ddRegistry.monitorDatasets(envProvider);

        Thread.sleep(1000);

        ScriptBundle bundle = new ScriptBundle.Builder().createScript().setName(RETAIL_SCRIPT_NAME)
                .setScript(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION))
                .setImportSchema(RETAIL_IMPORT_SCHEMA_FILE)
                .asMain()
                .add().build();

//        collectStats(ddRegistry);
//        importSchema(ddRegistry, bundle);
        end2endExample(ddRegistry, bundle);
//        simpleDBPipeline(ddRegistry);
        graphqlTest();
//        testDB();
    }

    private static Connection getConnection() throws Exception {
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(jdbcOptions);
        return provider.getOrEstablishConnection();
    }

    public static void simpleTest() throws Exception {
        StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<Integer> integers = flinkEnv.fromElements(12, 5);

//        DataStream<Row> rows = integers.map(i -> Row.of("Name"+i, i));

//  This alternative way of constructing this data stream produces the expected table schema
        Row row1 = Row.of("Mary",5);
        TypeInformation[] typeArray = new TypeInformation[2];
        typeArray[0] = BasicTypeInfo.STRING_TYPE_INFO;
        typeArray[1] = BasicTypeInfo.INT_TYPE_INFO;
        RowTypeInfo typeinfo = new RowTypeInfo(typeArray,new String[]{"name","number"});
        DataStream<Row> rows = flinkEnv.fromCollection(
              List.of(row1), typeinfo
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(flinkEnv);
        Table table = tableEnv.fromDataStream(rows);
        table.printSchema();

        rows.addSink(new PrintSinkFunction<>());

        flinkEnv.execute();
    }

    public static void end2endExample(DataSourceRegistry ddRegistry, ScriptBundle bundle) throws Exception {
        SqmlScript sqml = bundle.getMainScript();

        //Setup user schema and getting schema from statistics data
        SchemaImport schemaImporter = new SchemaImport(ddRegistry, Constraint.FACTORY_LOOKUP);
        Map<Name, FlexibleDatasetSchema> userSchema = schemaImporter.convertImportSchema(
            sqml.parseSchema());
        Preconditions.checkArgument(!schemaImporter.getErrors().isFatal(),
            schemaImporter.getErrors());
        ImportManager sqmlImporter = new ImportManager(ddRegistry);
        sqmlImporter.registerUserSchema(userSchema);

        ProcessBundle<ProcessMessage> errors = new ProcessBundle<>();
        LogicalPlanImpl logicalPlan = new LogicalPlanImpl();
        //1. Imports
        Name ordersName = toName(RETAIL_TABLE_NAMES[1]);
        ImportResolver importer = new ImportResolver(sqmlImporter, logicalPlan, errors);
        importer.resolveImport(ImportResolver.ImportMode.TABLE, toName(RETAIL_DATASET),
            Optional.of(ordersName), Optional.empty());
        //2. SQRL statements
        ai.dataeng.sqml.planner.Table orders = (ai.dataeng.sqml.planner.Table) logicalPlan.getSchemaElement(ordersName);
        Column ordersTime = (Column) orders.getField(toName("time"));
        RowExpression predicate = null; //TODO: create expression
        FilterOperator filter = new FilterOperator(orders.getCurrentNode(), predicate);
        orders.getCurrentNode().addConsumer(filter);
        ai.dataeng.sqml.planner.Table customerNoOrders = logicalPlan.createTable(toName("CustomerOrderStats"),
            false);
        Column customerid = (Column) orders.getField(toName("customerid"));
        AggregateOperator countAgg = AggregateOperator.createAggregateAndPopulateTable(filter,
            customerNoOrders,
            Map.of(customerid.getName(), new ColumnReferenceExpression(customerid)),
            Map.of(toName("num_orders"), new AggregateOperator.Aggregation(
                AggregateOperator.AggregateFunction.COUNT,
                List.of(new ColumnReferenceExpression(customerid)))));
        filter.addConsumer(countAgg);
        //3. Queries
        QueryAnalyzer.addDevModeQueries(logicalPlan);

        Preconditions.checkArgument(!errors.isFatal());

        LogicalPlanOptimizer.Result optimized = new SimpleOptimizer().optimize(logicalPlan);
        SQLGenerator.Result sql = new SQLGenerator(sqlConfig).generateDatabase(optimized);
        sql.executeDMLs();
        StreamExecutionEnvironment flinkEnv = new FlinkGenerator(flinkConfig,
            envProvider).generateStream(optimized, sql.getSinkMapper());
        flinkEnv.execute();

        //Print out contents of tables in H2
        Set<String> tableNames = Set.copyOf(sql.getToTable().values());
        try (Connection conn = sqlConfig.getConnection()) {
            for (String tableName : tableNames) {
                System.out.println("== " + tableName + "==");
                printTableContent(conn,tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load database driver",e);
        }

    }

    public static void graphqlTest() throws Exception {

        ///Graphql bit

        SchemaParser schemaParser = new SchemaParser();
        URL url = com.google.common.io.Resources.getResource("c360-small.graphqls");
        String schema = com.google.common.io.Resources.toString(url, Charsets.UTF_8);
        TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

        graphql.schema.idl.SchemaGenerator schemaGenerator = new graphql.schema.idl.SchemaGenerator();

        VertxOptions vertxOptions = new VertxOptions();
        VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);

        //In memory pool, if connection times out then the data is erased
        Pool client = JDBCPool.pool(
            vertx,
            // configure the connection
            new JDBCConnectOptions()
                // H2 connection string
                .setJdbcUrl(JDBC_URL)
                // username
//                .setUser("sa")
                // password
//                .setPassword("")
                ,
            // configure the pool
            new PoolOptions()
                .setMaxSize(1)
        );

        H2Column ordersPk = new UUIDColumn("_uuid_0", "_uuid_0"); //todo: PK identifier
        H2Column columnC = new IntegerColumn("customerid", "customerid_0");
        H2Table ordersTable = new H2Table(new Columns(List.of(columnC, ordersPk)), "orders_1", Optional.empty());

        H2Column column = new IntegerColumn("discount", "discount_0");
        H2Table entries = new H2Table(new Columns(List.of(column)), "orders_entries_2",
            Optional.of(new EqualsCriteria("_uuid_0", "_uuid_0")));

        H2Table customerOrderStats = new H2Table(new Columns(List.of(
            new IntegerColumn("customerid", "customerid_0"),
            new IntegerColumn("num_orders", "num_orders_0")
        )), "customerorderstats_3",
            Optional.empty());

        DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, RuntimeWiring.newRuntimeWiring()
            .codeRegistry(GraphQLCodeRegistry.newCodeRegistry()
                .dataFetcher(FieldCoordinates.coordinates("Query", "orders"),
                    new DefaultDataFetcher(new JdbcPool(client), new NoPage(), ordersTable))
                .dataFetcher(FieldCoordinates.coordinates("orders", "entries"),
                    new DefaultDataFetcher(/*dataLoaderRegistry, */new JdbcPool(client), new SystemPageProvider(), entries))
                .dataFetcher(FieldCoordinates.coordinates("Query", "CustomerOrderStats"),
                    new DefaultDataFetcher(new JdbcPool(client), new NoPage(), customerOrderStats))
                .build())
            .build());

//    System.out.println(new SchemaPrinter().print(graphQLSchema));

        GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema)
            .build();

        ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(
                "query Test {\n"
                    + "    CustomerOrderStats { customerid, num_orders }\n"
                    + "    orders(limit: 2) {\n"
                    + "        customerid"
                    + "        entries(order_by: [{discount: DESC}]) {\n"
                    + "            data {\n"
                    + "                discount\n"
                    + "            } \n"
                    + "            pageInfo { \n"
                    + "                cursor\n"
                    + "                hasNext\n"
                    + "             }\n"
                    + "        }\n"
                    + "    }\n"
                    + "}")
            .dataLoaderRegistry(dataLoaderRegistry)
            .build();
        ExecutionResult executionResult = graphQL.execute(executionInput);

        Object data = executionResult.getData();
        System.out.println();
        System.out.println(data);
        List<GraphQLError> errors2 = executionResult.getErrors();
        System.out.println(errors2);

        vertx.close();
    }

    public static void printTableContent(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + DatabaseUtil.sqlName(tableName));
            System.out.println(DatabaseUtil.result2String(rs));
        }
    }


    public static void importSchema(DataSourceRegistry ddRegistry, ScriptBundle bundle) throws Exception {
        SqmlScript sqml = bundle.getMainScript();

        SchemaImport schemaImporter = new SchemaImport(ddRegistry, Constraint.FACTORY_LOOKUP);
        Map<Name, FlexibleDatasetSchema> userSchema = schemaImporter.convertImportSchema(sqml.parseSchema());

        if (schemaImporter.hasErrors()) {
            System.out.println("Import errors:");
            schemaImporter.getErrors().forEach(e -> System.out.println(e));
        }

        System.out.println("-------Import Schema-------");
        System.out.print(toString(userSchema));

        ImportManager sqmlImporter = new ImportManager(ddRegistry);
        sqmlImporter.registerUserSchema(userSchema);

        sqmlImporter.importAllTable(RETAIL_DATASET);

        ProcessBundle<SchemaConversionError> errors = new ProcessBundle<>();
        ImportSchema schema = sqmlImporter.createImportSchema(errors);

        if (errors.hasErrors()) {
            System.out.println("-------Import Errors-------");
            errors.forEach(e -> System.out.println(e));
        }

        System.out.println("-------Script Schema-------");
        System.out.println(schema.getSchema());
        System.out.println("-------Tables-------");
        for (Map.Entry<Name, ImportSchema.Mapping> entry : schema.getMappings().entrySet()) {
            Preconditions.checkArgument(entry.getValue().isTable() && entry.getValue().isSource());
            ImportSchema.SourceTableImport tableImport = schema.getSourceTable(entry.getKey());
            Preconditions.checkNotNull(tableImport);
            System.out.println("Table name: " + entry.getKey());
            System.out.print(toString(Name.system("local"),singleton(tableImport.getSourceSchema())));
        }
    }

    private static String toString(Name name, FlexibleDatasetSchema schema) {
        return toString(Collections.singletonMap(name,schema));
    }

    private static String toString(Map<Name, FlexibleDatasetSchema> schema) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        SchemaExport exporter = new SchemaExport();
        SchemaDefinition sd = exporter.export(schema);

        StringWriter s = new StringWriter();
        try {
            mapper.writeValue(s, sd);
        } catch (IOException e ) {
            throw new RuntimeException(e);
        }
        return s.toString();
    }

    public static void collectStats(DataSourceRegistry ddRegistry) throws Exception {
        ddRegistry.monitorDatasets(envProvider);

        Thread.sleep(1000);

        SourceDataset dd = ddRegistry.getDataset(RETAIL_DATASET);
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        FlexibleDatasetSchema.Builder dataset = new FlexibleDatasetSchema.Builder();
        //Retrieve the collected statistics
        for (String table : RETAIL_TABLE_NAMES) {
            if (dd.getTable(table) == null) {
                System.out.println(String.format("Table is null %s", table));
                continue;
            }
            SourceTableStatistics tableStats = ddRegistry.getTableStatistics(dd.getTable(table));
            Name tableName = Name.system(table);
            dataset.add(schemaGenerator.mergeSchema(tableStats, tableName, NamePath.of(tableName)));
        }

        if (schemaGenerator.hasErrors()) {
            System.out.println("## Generator errors:");
            schemaGenerator.getErrors().forEach(e -> System.out.println(e));
        }

        System.out.println("-------Exported JSON-------");
        System.out.print(toString(Name.system("ecommerce"),dataset.build()));

    }

    private static FlexibleDatasetSchema singleton(FlexibleDatasetSchema.TableField table) {
        return new FlexibleDatasetSchema.Builder().add(table).build();
    }

/*
    public static void simpleDBPipeline(DataSourceRegistry ddRegistry) throws Exception {
        StreamExecutionEnvironment flinkEnv = envProvider.create();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(flinkEnv);


        JDBCSinkFactory dbSinkFactory = new JDBCSinkFactory(jdbcOptions, SQLDialect.H2);

        SourceDataset dd = ddRegistry.getDataset(RETAIL_DATA_DIR_NAME);

        Map<String, NamePathOld[]> imports = new HashMap<>();
        imports.put(RETAIL_TABLE_NAMES[0],new NamePathOld[]{NamePathOld.ROOT}); //Customer
        imports.put(RETAIL_TABLE_NAMES[1],new NamePathOld[]{NamePathOld.ROOT, NamePathOld.of("entries")}); //Order
        imports.put(RETAIL_TABLE_NAMES[2],new NamePathOld[]{NamePathOld.ROOT}); //Product

        Map<String,Table> shreddedImports = new HashMap<>();

        for (Map.Entry<String, NamePathOld[]> importEntry : imports.entrySet()) {
            String tableName = importEntry.getKey();
            SourceTable stable = dd.getTable(tableName);
            SourceTableStatistics tableStats = ddRegistry.getTableStatistics(stable);
            SourceTableSchema tableSchema = tableStats.getSchema();

            DataStream<SourceRecord> stream = stable.getDataStream(flinkEnv);
            final OutputTag<SchemaValidationError> schemaErrorTag = new OutputTag<>("schema-error-"+tableName){};
            SingleOutputStreamOperator<SourceRecord> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, tableSchema, SchemaAdjustmentSettings.DEFAULT));
            validate.getSideOutput(schemaErrorTag).addSink(new PrintSinkFunction<>()); //TODO: handle errors

            for (NamePathOld shreddingPath : importEntry.getValue()) {
                RecordShredder shredder = RecordShredder.from(shreddingPath, tableSchema);
                SingleOutputStreamOperator<Row> process = validate.flatMap(shredder.getProcess(),
                        FlinkUtilities.convert2RowTypeInfo(shredder.getResultSchema()));

                process.addSink(new PrintSinkFunction<>()); //TODO: remove, debugging only

                Table table = tableEnv.fromDataStream(process);
                table.printSchema();

                String shreddedTableName = tableName;
                if (shreddingPath.getNumComponents()>0) shreddedTableName += "_" + shreddingPath.toString('_');

                shreddedImports.put(shreddedTableName, table);

                tableEnv.toRetractStream(table, Row.class).flatMap(new RowMapFunction())
                        .addSink(dbSinkFactory.getSink(shreddedTableName,shredder.getResultSchema()));

            }
        }

//        Table OrderEntries = shreddedImports.get("Orders_entries");
//        Table Product = shreddedImports.get("Product");

//        Table po = OrderEntries.groupBy($("productid")).select($("productid"),$("quantity").sum().as("quantity"));
//
//        DestinationTableSchema poschema = DestinationTableSchema.builder().add(DestinationTableSchema.Field.primaryKey("productid", IntegerType.INSTANCE))
//                        .add(DestinationTableSchema.Field.simple("quantity",IntegerType.INSTANCE)).build();
//        tableEnv.toRetractStream(Product, Row.class).flatMap(new RowMapFunction()).addSink(dbSinkFactory.getSink("po_count",poschema));


        flinkEnv.execute();
    }
    */




}
