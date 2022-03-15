package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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
        for (DataSourceConfiguration dsConfig : persistence.getDatasets()) {
            ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
            addOrUpdateSource(dsConfig, errors, 10);
            if (errors.isFatal()) {
                throw new RuntimeException(
                        errors.combineMessages(ProcessMessage.Severity.FATAL,
                                "Could not initialize dataset from stored config: \n","\n"));
            }
        }
    }

    public synchronized SourceDataset addOrUpdateSource
            (@NonNull DataSourceConfiguration datasource,
             @NonNull ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        return addOrUpdateSource(datasource,errors, defaultInitialPollingWaitMS);
    }

    public synchronized SourceDataset addOrUpdateSource
            (@NonNull DataSourceConfiguration datasource,
             @NonNull ProcessMessage.ProcessBundle<ConfigurationError> errors,
             long sourceInitializationWaitTimeMS) {
        DataSource source = datasource.initialize(errors);
        if (source==null) return null;
        SourceDataset dataset = datasets.get(source.getDatasetName());
        if (dataset==null) {
            dataset = new SourceDataset(this, source);
            persistence.putDataset(source.getDatasetName(), datasource);
            datasets.put(dataset.getName(), dataset);

        } else {
            if (dataset.getSource().isCompatible(source, errors)) {
                dataset.updateConfiguration(source);
            } else {
                return null;
            }
            source = dataset.getSource();
        }
        long initialPollingDelayMS = pollTablesEveryMS;
        try {
            dataset.refreshTables(sourceInitializationWaitTimeMS);
        } catch (InterruptedException e) {
            //Initial table polling is taking too long, let's do it again soon in the background
            initialPollingDelayMS = 0;
        }
        tableMonitors.scheduleWithFixedDelay(dataset.polling, initialPollingDelayMS, pollTablesEveryMS, TimeUnit.MILLISECONDS);
        return dataset;
    }

    @Override
    public SourceDataset getDataset(@NonNull Name name) {
        return datasets.get(name);
    }

    public SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

    public Collection<SourceDataset> getDatasets() {
        return datasets.values();
    }

    @Override
    public void close() throws IOException {
        tableMonitors.shutdown();
    }

}
