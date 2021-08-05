package ai.dataeng.sqml;

import ai.dataeng.sqml.ingest.DataSourceMonitor;
import ai.dataeng.sqml.flink.DefaultEnvironmentProvider;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;

import java.nio.file.Paths;

public class Main2 {

    public static final String RETAIL_DIR = System.getProperty("user.dir") + "/sqml-examples/retail/";
    public static final String RETAIL_DATA_DIR = RETAIL_DIR + "ecommerce-data";

    public static void main(String[] args) throws Exception {
        DirectoryDataset dd = new DirectoryDataset(Paths.get(RETAIL_DATA_DIR));
        final DataSourceMonitor env = new DataSourceMonitor(new DefaultEnvironmentProvider());
        env.addDataset(dd);


    }
}
