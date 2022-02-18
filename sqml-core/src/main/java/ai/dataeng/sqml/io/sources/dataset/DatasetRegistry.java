package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
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
    final long defaultInitialPollingWaitMS = 50;
    final long pollingBackgroundWaitTimeMS = 5000;
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
        addSource(datasource,defaultInitialPollingWaitMS);
    }

    public synchronized void addSource(@NonNull DataSourceConfiguration datasource, long sourceInitializationWaitTimeMS) {
        Preconditions.checkArgument(datasource.validate(new ProcessMessage.ProcessBundle<>()),
                "Provided data source configuration is invalid: %s", datasource);
        DataSource source = datasource.initialize();

        Preconditions.checkArgument(!datasets.containsKey(source.getDatasetName()),
                "A data source with the name [%s] already exists",source.getDatasetName());
        SourceDataset ds = new SourceDataset(this, source);
        persistence.putDataset(source.getDatasetName(),datasource);
        datasets.put(ds.getName(),ds);
        long initialPollingDelayMS = pollTablesEveryMS;
        try {
            ds.refreshTables(sourceInitializationWaitTimeMS);
        } catch (InterruptedException e) {
            //Initial table polling is taking too long, let's do it again soon in the background
            initialPollingDelayMS = 0;
        }
        tableMonitors.scheduleWithFixedDelay(ds.polling, initialPollingDelayMS, pollTablesEveryMS, TimeUnit.MILLISECONDS);
    }

    public synchronized void updateSource(@NonNull DataSourceConfiguration datasource) {
        Preconditions.checkArgument(datasource.validate(new ProcessMessage.ProcessBundle<>()),
                "Provided data source configuration is invalid: %s", datasource);
        DataSource source = datasource.initialize();

        SourceDataset existing = datasets.get(source.getDatasetName());
        Preconditions.checkArgument(existing!=null,"Datasource has not yet been connected: [%s]", source.getDatasetName());
        existing.updateConfiguration(source);
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
