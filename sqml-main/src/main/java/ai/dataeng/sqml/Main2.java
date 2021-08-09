package ai.dataeng.sqml;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.ingest.DataSourceMonitor;
import ai.dataeng.sqml.flink.DefaultEnvironmentProvider;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main2 {

    public static final String RETAIL_DIR = System.getProperty("user.dir") + "/sqml-examples/retail/";
    public static final String RETAIL_DATA_DIR = RETAIL_DIR + "ecommerce-data";

    public static final Path outputBase = Path.of("tmp","datasource");


    public static void main(String[] args) throws Exception {
        DirectoryDataset dd = new DirectoryDataset(Paths.get(RETAIL_DATA_DIR));
        HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
        final DataSourceMonitor env = new DataSourceMonitor(new DefaultEnvironmentProvider(), kvStoreFactory);
        env.addDataset(dd);

        Thread.sleep(1000);

        //Retrieve the collected statistics
        System.out.println(env.getTableStatistics(dd.getTable("Customer")));
        System.out.println(env.getTableStatistics(dd.getTable("Order")));
        System.out.println(env.getTableStatistics(dd.getTable("Product")));

    }
}
