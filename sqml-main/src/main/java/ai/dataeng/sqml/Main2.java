package ai.dataeng.sqml;

import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.source.SourceTable;
import ai.dataeng.sqml.source.SourceTableListener;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.io.IOException;
import java.nio.file.Paths;

public class Main2 {

    public static final String RETAIL_DIR = System.getProperty("user.dir") + "/sqml-examples/retail/";
    public static final String RETAIL_DATA_DIR = RETAIL_DIR + "ecommerce-data";

    public static void main(String[] args) throws Exception {
        DirectoryDataset dd = new DirectoryDataset(Paths.get(RETAIL_DATA_DIR));
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        dd.addSourceTableListener(new SourceTableListener() {
            @Override
            public void registerSourceTable(SourceTable sourceTable) throws DuplicateException {
                DataStream<SourceRecord> data = sourceTable.getDataStream(env);
                data.addSink(new PrintSinkFunction<>());
            }
        });
        env.execute("test");
    }
}
