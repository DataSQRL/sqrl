package ai.dataeng.sqml;

import static org.apache.flink.table.api.Expressions.$;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.db.tabular.JDBCSinkFactory;
import ai.dataeng.sqml.db.tabular.RowMapFunction;
import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.DatasetLookup;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.NamePathOld;
import ai.dataeng.sqml.ingest.schema.*;
import ai.dataeng.sqml.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.ingest.schema.external.SchemaExport;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.ingest.shredding.RecordShredder;
import ai.dataeng.sqml.ingest.source.SourceTableListener;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.ingest.source.SourceTable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.*;

import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.type.IntegerType;
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
    public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
    public static final String RETAIL_SCRIPT_NAME = "c360";
    public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
    public static final String SQML_SCRIPT_EXTENSION = ".sqml";
    public static final Path RETAIL_PRE_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");

    public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Orders", "Product"};

    public static final Path outputBase = Path.of("tmp","datasource");
    public static final Path dbPath = Path.of("tmp","output");

    private static final EnvironmentFactory envProvider = new DefaultEnvironmentFactory();

    private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
            .withDriverName("org.h2.Driver")
            .build();

    public static void main(String[] args) throws Exception {
        readSchema();
//        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
//        DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
//        DirectoryDataset dd = new DirectoryDataset(DatasetRegistration.of(RETAIL_DATA_DIR.getFileName().toString()), RETAIL_DATA_DIR);
//        ddRegistry.addDataset(dd);
//
//        collectStats(ddRegistry);
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

    public static void readSchema() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        SchemaDefinition importSchema = mapper.readValue(RETAIL_PRE_SCHEMA_FILE.toFile(),
                SchemaDefinition.class);

        SchemaImport importer = new SchemaImport(new MockDatasetLookup(), Constraint.FACTORY_LOOKUP);
        Map<Name, FlexibleDatasetSchema> result = importer.convertImportSchema(importSchema);

        if (importer.hasErrors()) {
            System.out.println("Import errors:");
            importer.getErrors().forEach(e -> System.out.println(e));
        }

        result.forEach((k,v) -> {
            System.out.println(k.getDisplay());
            v.forEach(t -> System.out.println(t.getName()));
        });

        SchemaExport exporter = new SchemaExport();
        SchemaDefinition sd = exporter.export(result);

        System.out.println("-------Exported JSON-------");
        mapper.writeValue(System.out,sd);


//        for (DatasetDefinition dd : importSchema.datasets) {
//            System.out.println(dd.name);
//        }
    }

    public static class MockDatasetLookup implements DatasetLookup {

        @Override
        public SourceDataset getDataset(final Name name) {
            return new SourceDataset() {
                @Override
                public void addSourceTableListener(SourceTableListener listener) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Collection<? extends SourceTable> getTables() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SourceTable getTable(Name name) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public DatasetRegistration getRegistration() {
                    return new DatasetRegistration(name, NameCanonicalizer.LOWERCASE_ENGLISH);
                }
            };
        }
    }


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

                Table table = tableEnv.fromDataStream(process/*, Schema.newBuilder()
                .watermark("__timestamp", "SOURCE_WATERMARK()")
                .build()*/);
                table.printSchema();

                String shreddedTableName = tableName;
                if (shreddingPath.getNumComponents()>0) shreddedTableName += "_" + shreddingPath.toString('_');

                shreddedImports.put(shreddedTableName, table);

                tableEnv.toRetractStream(table, Row.class).flatMap(new RowMapFunction())
                        .addSink(dbSinkFactory.getSink(shreddedTableName,shredder.getResultSchema()));

            }
        }

        Table OrderEntries = shreddedImports.get("Orders_entries");
        Table Product = shreddedImports.get("Product");

        Table po = OrderEntries.groupBy($("productid")).select($("productid"),$("quantity").sum().as("quantity"));

        DestinationTableSchema poschema = DestinationTableSchema.builder().add(DestinationTableSchema.Field.primaryKey("productid", IntegerType.INSTANCE))
                        .add(DestinationTableSchema.Field.simple("quantity",IntegerType.INSTANCE)).build();
        tableEnv.toRetractStream(po, Row.class).flatMap(new RowMapFunction()).addSink(dbSinkFactory.getSink("po_count",poschema));


        flinkEnv.execute();
    }

    public static void collectStats(DataSourceRegistry ddRegistry) throws Exception {
        ddRegistry.monitorDatasets(envProvider);


        Thread.sleep(1000);

        String content = Files.readString(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION));
        Bundle sqml = new Bundle.Builder().setMainScript(RETAIL_SCRIPT_NAME, content).build();
        SourceDataset dd = ddRegistry.getDataset(RETAIL_DATA_DIR_NAME);

        //Retrieve the collected statistics
        for (String table : RETAIL_TABLE_NAMES) {
            if (dd.getTable(table) == null) {
                System.out.println(String.format("Table is null %s", table));
                continue;
            }
            SourceTableStatistics tableStats = ddRegistry.getTableStatistics(dd.getTable(table));
            SourceTableSchema schema = tableStats.getSchema();
            System.out.println(schema);
        }
    }


}
