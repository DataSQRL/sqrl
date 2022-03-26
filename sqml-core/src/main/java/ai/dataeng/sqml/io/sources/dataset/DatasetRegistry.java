package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetRegistry implements DatasetLookup, Closeable {

    final DatasetRegistryPersistence persistence;

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
            initializeSource(dsConfig.initialize(errors));
            ProcessMessage.ProcessBundle.logMessages(errors);
        }
    }

    private SourceDataset initializeSource(DataSource source) {
        if (source==null) return null;
        SourceDataset dataset = new SourceDataset(this, source);
        datasets.put(dataset.getName(), dataset);
        return dataset;
    }

    public synchronized SourceDataset addOrUpdateSource
            (@NonNull DataSourceConfiguration datasource,
             @NonNull ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        return addOrUpdateSource(datasource, Collections.EMPTY_LIST, errors);
    }


    public synchronized SourceDataset addOrUpdateSource
            (@NonNull DataSourceConfiguration datasource, @NonNull List<SourceTableConfiguration> tables,
             @NonNull ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        DataSource source = datasource.initialize(errors);
        if (source == null) return null; //validation failed
        SourceDataset dataset = datasets.get(source.getDatasetName());
        if (dataset==null) {
            dataset = initializeSource(source);
            source = dataset.getSource();
        } else {
            source = dataset.getSource();
            if (!source.update(datasource,errors)) {
                return null;
            }
        }
        persistence.putDataset(source.getDatasetName(), source.getConfiguration());

        Set<Name> tableNames = new HashSet<>();
        for (SourceTableConfiguration tbl : tables) {
            if (Name.validName(tbl.getName())) {
                tableNames.add(dataset.getCanonicalizer().name(tbl.getName()));
            }
        }

        List<SourceTableConfiguration> allTables = new ArrayList<>(tables);
        if (datasource.discoverTables()) {
            for (SourceTableConfiguration tbl: source.discoverTables(errors)) {
                Name tblName = dataset.getCanonicalizer().name(tbl.getName());
                if (!tableNames.contains(tblName)) {
                    allTables.add(tbl);
                }
            }
        }
        for (SourceTableConfiguration tbl : allTables) {
            dataset.addTable(tbl, errors);
        }
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

    }

}
