package ai.dataeng.sqml;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.execution.SQMLBundle;
import ai.dataeng.sqml.flink.EnvironmentProvider;
import ai.dataeng.sqml.flink.DefaultEnvironmentProvider;
import ai.dataeng.sqml.flink.SaveToKeyValueStoreSink;
import ai.dataeng.sqml.flink.util.BufferedLatestSelector;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.ingest.*;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main2 {

    public static final Path RETAIL_DIR = Path.of(System.getProperty("user.dir")).resolve("sqml-examples").resolve("retail");
    public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve("ecommerce-data");
    public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve("c360");
    public static final String RETAIL_SCRIPT_NAME = "c360";
    public static final String SQML_SCRIPT_EXTENSION = ".sqml";

    public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Order", "Product"};

    public static final Path outputBase = Path.of("tmp","datasource");

    private static final EnvironmentProvider envProvider = new DefaultEnvironmentProvider();

    public static void main(String[] args) throws Exception {
        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
        DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
        DirectoryDataset dd = new DirectoryDataset(RETAIL_DATA_DIR);
        ddRegistry.addDataset(dd);

        ddRegistry.monitorDatasets(envProvider);

        Thread.sleep(1000);

        String content = Files.readString(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION));
        SQMLBundle sqml = new SQMLBundle.Builder().setMainScript(RETAIL_SCRIPT_NAME, content).build();

        //Retrieve the collected statistics
        for (String table : RETAIL_TABLE_NAMES) {
            SourceTableStatistics tableStats = ddRegistry.getTableStatistics(dd.getTable(table));
            SourceTableSchema schema = tableStats.getSchema();
            System.out.println(schema);
        }

    }


}
