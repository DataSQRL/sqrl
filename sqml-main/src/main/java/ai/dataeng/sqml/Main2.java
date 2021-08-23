package ai.dataeng.sqml;

import static org.apache.flink.table.api.Expressions.$;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.flink.DefaultEnvironmentProvider;
import ai.dataeng.sqml.flink.EnvironmentProvider;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.ingest.RecordShredder;
import ai.dataeng.sqml.ingest.SchemaAdjustmentSettings;
import ai.dataeng.sqml.ingest.SchemaValidationError;
import ai.dataeng.sqml.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.ingest.SourceTableSchema;
import ai.dataeng.sqml.ingest.SourceTableStatistics;
import ai.dataeng.sqml.source.SourceDataset;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.source.SourceTable;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

public class Main2 {

    public static final Path RETAIL_DIR = Path.of(System.getProperty("user.dir")).resolve("sqml-examples").resolve("retail");
    public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
    public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
    public static final String RETAIL_SCRIPT_NAME = "c360";
    public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
    public static final String SQML_SCRIPT_EXTENSION = ".sqml";

    public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Order", "Product"};

    public static final Path outputBase = Path.of("tmp","datasource");

    private static final EnvironmentProvider envProvider = new DefaultEnvironmentProvider();

    public static void main(String[] args) throws Exception {
        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
        DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
        DirectoryDataset dd = new DirectoryDataset(RETAIL_DATA_DIR);
        ddRegistry.addDataset(dd);

//        collectStats(ddRegistry);
        simpleDBPipeline(ddRegistry);

//        simpleTest();
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
        StreamExecutionEnvironment flinkEnv = envProvider.get();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(flinkEnv);

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
            }
        }

        shreddedImports.keySet().forEach(t -> System.out.println(t));
        Table OrderEntries = shreddedImports.get("Order_entries");
        Table Product = shreddedImports.get("Product");

        Table productOrders = OrderEntries.groupBy($("productid")).select($("productid"),$("quantity").sum().as("quantity"));

        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(productOrders, Row.class);
        result.print();

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
