package ai.dataeng.sqml;

import static org.apache.flink.table.api.Expressions.$;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.db.tabular.JDBCSinkFactory;
import ai.dataeng.sqml.db.tabular.RowMapFunction;
import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.ingest.shredding.RecordShredder;
import ai.dataeng.sqml.ingest.schema.SchemaAdjustmentSettings;
import ai.dataeng.sqml.ingest.schema.SchemaValidationError;
import ai.dataeng.sqml.ingest.schema.SchemaValidationProcess;
import ai.dataeng.sqml.ingest.schema.SourceTableSchema;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.source.SourceDataset;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.source.SourceTable;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.*;

import ai.dataeng.sqml.type.IntegerType;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
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

    public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Orders", "Product"};

    public static final Path outputBase = Path.of("tmp","datasource");
    public static final Path dbPath = Path.of("tmp","output");

    private static final EnvironmentFactory envProvider = new DefaultEnvironmentFactory();

    private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
            .withDriverName("org.h2.Driver")
            .build();

    public static void main(String[] args) throws Exception {
        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
        DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
        DirectoryDataset dd = new DirectoryDataset(RETAIL_DATA_DIR);
        ddRegistry.addDataset(dd);

        collectStats(ddRegistry);
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



    public static void simpleDBPipeline(DataSourceRegistry ddRegistry) throws Exception {
        StreamExecutionEnvironment flinkEnv = envProvider.create();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(flinkEnv);


        JDBCSinkFactory dbSinkFactory = new JDBCSinkFactory(jdbcOptions, SQLDialect.H2);

        SourceDataset dd = ddRegistry.getDataset(RETAIL_DATA_DIR_NAME);

        Map<String,NamePath[]> imports = new HashMap<>();
        imports.put(RETAIL_TABLE_NAMES[0],new NamePath[]{NamePath.ROOT}); //Customer
        imports.put(RETAIL_TABLE_NAMES[1],new NamePath[]{NamePath.ROOT, NamePath.of("entries")}); //Order
        imports.put(RETAIL_TABLE_NAMES[2],new NamePath[]{NamePath.ROOT}); //Product

        Map<String,Table> shreddedImports = new HashMap<>();

        for (Map.Entry<String,NamePath[]> importEntry : imports.entrySet()) {
            String tableName = importEntry.getKey();
            SourceTable stable = dd.getTable(tableName);
            SourceTableStatistics tableStats = ddRegistry.getTableStatistics(stable);
            SourceTableSchema tableSchema = tableStats.getSchema();

            DataStream<SourceRecord> stream = stable.getDataStream(flinkEnv);
            final OutputTag<SchemaValidationError> schemaErrorTag = new OutputTag<>("schema-error-"+tableName){};
            SingleOutputStreamOperator<SourceRecord> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, tableSchema, SchemaAdjustmentSettings.DEFAULT));
            validate.getSideOutput(schemaErrorTag).addSink(new PrintSinkFunction<>()); //TODO: handle errors

            for (NamePath shreddingPath : importEntry.getValue()) {
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
