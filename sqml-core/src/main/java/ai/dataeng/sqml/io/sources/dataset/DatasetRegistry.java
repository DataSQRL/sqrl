package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.config.error.ErrorPrefix;
import ai.dataeng.sqml.io.sinks.DataSink;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.DataSourceUpdate;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.config.error.ErrorCollector;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

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

    private void initializeDatasets() {
        //Read existing datasets from store
        for (DatasetRegistryPersistence.DataSourceStorage configEntry : persistence.getDatasets()) {
            ErrorCollector errors = ErrorCollector.fromPrefix(ErrorPrefix.INITIALIZE);
            initializeSource(configEntry.getConfig().initialize(configEntry.getName(), errors));
            errors.log();
        }
    }

    private SourceDataset initializeSource(DataSource source) {
        if (source==null) return null;
        SourceDataset dataset = new SourceDataset(this, source);
        datasets.put(dataset.getName(), dataset);
        return dataset;
    }

    public synchronized SourceDataset addOrUpdateSource
            (@NonNull String name, @NonNull DataSourceConfiguration datasource,
             @NonNull ErrorCollector errors) {
        return addOrUpdateSource(DataSourceUpdate.builder().name(name).config(datasource).build(), errors);
    }


    public synchronized SourceDataset addOrUpdateSource
            (@NonNull DataSourceUpdate update,
             @NonNull ErrorCollector errors) {
        DataSource source = update.getConfig().initialize(update.getName(), errors);
        if (source == null) return null; //validation failed
        SourceDataset dataset = datasets.get(source.getDatasetName());
        if (dataset==null) {
            dataset = initializeSource(source);
            source = dataset.getSource();
        } else {
            source = dataset.getSource();
            if (!source.update(update.getConfig(),errors)) {
                return null;
            }
        }
        persistence.putDataset(source.getDatasetName(), source.getConfiguration());

        Set<Name> tableNames = new HashSet<>();
        for (SourceTableConfiguration tbl : update.getTables()) {
            if (Name.validName(tbl.getName())) {
                tableNames.add(dataset.getCanonicalizer().name(tbl.getName()));
            }
        }

        List<SourceTableConfiguration> allTables = new ArrayList<>(update.getTables());
        if (update.isDiscoverTables()) {
            for (SourceTableConfiguration tbl: source.discoverTables(errors)) {
                Name tblName = dataset.getCanonicalizer().name(tbl.getName());
                if (!tableNames.contains(tblName)) {
                    allTables.add(tbl);
                }
            }
        }

        errors = errors.resolve(source.getDatasetName());
        for (SourceTableConfiguration tbl : allTables) {
            dataset.addTable(tbl, errors);
        }
        return dataset;
    }

    public synchronized SourceDataset removeSource(@NonNull Name name) {
        SourceDataset source = datasets.remove(name);
        if (source != null) {
            persistence.removeDataset(name);
            List<SourceTable> tables = new ArrayList<>(source.getTables());
            for (SourceTable tbl : tables) {
                source.removeTable(tbl.getName());
            }
        }
        return source;
    }

    public SourceDataset removeSource(@NonNull String name) {
        return Name.getIfValidSystemName(name,this::removeSource);
    }

    @Override
    public SourceDataset getDataset(@NonNull Name name) {
        return datasets.get(name);
    }

    public Collection<SourceDataset> getDatasets() {
        return datasets.values();
    }

    @Override
    public void close() throws IOException {

    }

}
