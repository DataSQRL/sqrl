package ai.dataeng.sqml;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.db.tabular.JDBCSinkFactory;
import ai.dataeng.sqml.execution.SQMLBundle;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaAdjustmentSettings;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.SchemaValidationProcess;
import ai.dataeng.sqml.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.ingest.schema.external.SchemaExport;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.ingest.shredding.RecordShredder;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.ingest.stats.SchemaGenerator;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.schema2.basic.BasicTypeManager;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.*;

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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

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

    private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
            .withDriverName("org.h2.Driver")
            .build();

    public static void main(String[] args) throws Exception {
        BasicTypeManager.detectType("test");

        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
        DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
        DirectoryDataset dd = new DirectoryDataset(DatasetRegistration.of(RETAIL_DATASET), RETAIL_DATA_DIR);
        ddRegistry.addDataset(dd);

        ddRegistry.monitorDatasets(envProvider);

        Thread.sleep(1000);

        SQMLBundle bundle = new SQMLBundle.Builder().createScript().setName(RETAIL_SCRIPT_NAME)
                .setScript(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION))
                .setImportSchema(RETAIL_IMPORT_SCHEMA_FILE)
                .asMain()
                .add().build();

//        collectStats(ddRegistry);
//        importSchema(ddRegistry, bundle);
        tableShredding(ddRegistry, bundle);
//        simpleDBPipeline(ddRegistry);

//        testDB();
    }

    private static Connection getConnection() throws Exception {
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(jdbcOptions);
        return provider.getOrEstablishConnection();
    }

    public static void testDB() throws Exception {
        Connection conn = getConnection();
        DSLContext dsl = DSL.using(conn,SQLDialect.H2);

//        dsl.meta().getTables().stream().map(t -> t.getName()).forEach( n -> System.out.println(n));

        //Why does this not work when the Customer table is listed above???
        for (String tableName : RETAIL_TABLE_NAMES) {
            tableName = JDBCSinkFactory.sqlName(tableName);
            for (Record r : dsl.select().from(tableName).fetch()) {
                System.out.println(r);
            }
        }
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

    public static void tableShredding(DataSourceRegistry ddRegistry, SQMLBundle bundle) throws Exception {
        SQMLBundle.SQMLScript sqml = bundle.getMainScript();

        SchemaImport schemaImporter = new SchemaImport(ddRegistry, Constraint.FACTORY_LOOKUP);
        Map<Name, FlexibleDatasetSchema> userSchema = schemaImporter.convertImportSchema(sqml.parseSchema());

        Preconditions.checkArgument(!schemaImporter.getErrors().isFatal());

        ImportManager sqmlImporter = new ImportManager(ddRegistry);
        sqmlImporter.registerUserSchema(userSchema);
        sqmlImporter.importAllTable(RETAIL_DATASET);

        ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
        ImportSchema schema = sqmlImporter.createImportSchema(errors);

        Preconditions.checkArgument(!errors.isFatal());
        StreamExecutionEnvironment flinkEnv = envProvider.create();
        JDBCSinkFactory dbSinkFactory = new JDBCSinkFactory(jdbcOptions, SQLDialect.H2);

        Set<String> tableNames = new HashSet<>();

        for (String tableName : RETAIL_TABLE_NAMES) {
            ImportSchema.SourceTableImport tableImport = schema.getSourceTable(Name.system(tableName));
            Preconditions.checkNotNull(tableImport);
//            System.out.print(toString(Name.system("local"),singleton(tableImport.getSourceSchema())));

            DataStream<SourceRecord<String>> stream = tableImport.getTable().getDataStream(flinkEnv);
            final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>("schema-error-"+tableName){};
            SingleOutputStreamOperator<SourceRecord<Name>> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, tableImport.getSourceSchema(),
                    SchemaAdjustmentSettings.DEFAULT, tableImport.getTable().getDataset().getRegistration()));
            validate.getSideOutput(schemaErrorTag).addSink(new PrintSinkFunction<>()); //TODO: handle errors

            for (RecordShredder shredder : RecordShredder.from(tableImport.getTableSchema())) {
                SingleOutputStreamOperator<Row> process = validate.flatMap(shredder.getProcess());

                String shreddedTableName = tableName;
                if (shredder.getTableIdentifier().getLength()>0) {
                    shreddedTableName += "_" + shredder.getTableIdentifier().toString('_');
                }
                tableNames.add(shreddedTableName);

                process.addSink(new PrintSinkFunction<>()); //TODO: remove, debugging only
                process.addSink(dbSinkFactory.getSink(shreddedTableName,shredder.getResultSchema()));
            }
        }

        flinkEnv.execute();

        //Print out contents of tables in H2
        for (String shreddedTable : tableNames) {
            System.out.println("== " + shreddedTable + "==");
            for (Record r : dbSinkFactory.getTableContent(shreddedTable)) {
                System.out.println(r);
            }
        }
    }

    public static void importSchema(DataSourceRegistry ddRegistry, SQMLBundle bundle) throws Exception {
        SQMLBundle.SQMLScript sqml = bundle.getMainScript();

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

        ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
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
