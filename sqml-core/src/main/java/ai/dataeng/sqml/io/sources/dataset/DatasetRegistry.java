package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DatasetRegistry implements DatasetLookup, Closeable {

    final DatasetRegistryPersistence persistence;

    private final ScheduledExecutorService tableMonitors = Executors.newSingleThreadScheduledExecutor();
    final long pollingWaitTimeMS = 5000;
    final long pollTablesEveryMS = 300000;
    final SourceTableMonitor tableMonitor;

    private final Map<Name, SourceDataset> datasets;


    public DatasetRegistry(DatasetRegistryPersistence persistence, SourceTableMonitor tableMonitor) {
        this.persistence = persistence;
        this.tableMonitor = tableMonitor;
        this.datasets = new HashMap<>();
        initializeDatasets();
    }

    void initializeDatasets() {
        //Read existing datasets from store
        for (DataSourceConfiguration dsConfig : persistence.getDatasets()) addSource(dsConfig);
    }


    public synchronized void addSource(@NonNull DataSourceConfiguration datasource) {
        Preconditions.checkArgument(!datasets.containsKey(datasource.getDatasetName()),
                "A data source with the name [%s] already exists",datasource.getDatasetName());
        SourceDataset ds = new SourceDataset(this, datasource);
        persistence.putDataset(datasource);
        datasets.put(ds.getName(),ds);
        tableMonitors.scheduleWithFixedDelay(ds.polling, 0, pollTablesEveryMS, TimeUnit.MILLISECONDS);
    }

    public synchronized void updateSource(@NonNull DataSourceConfiguration datasource) {
        SourceDataset existing = datasets.get(datasource.getDatasetName());
        Preconditions.checkArgument(existing!=null,"Datasource has not yet been connected: [%s]",datasource.getDatasetName());
        existing.updateConfiguration(datasource);
    }

    @Override
    public SourceDataset getDataset(@NonNull Name name) {
        return datasets.get(name);
    }

    public SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

    @Override
    public void close() throws IOException {
        tableMonitors.shutdown();
    }

}
