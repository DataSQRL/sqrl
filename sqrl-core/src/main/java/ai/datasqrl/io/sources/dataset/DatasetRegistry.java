package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSource;
import ai.datasqrl.io.sources.DataSourceImplementation;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.parse.tree.name.Name;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class DatasetRegistry implements Closeable {

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
    for (DataSource source : persistence.getDatasets()) {
      initializeSource(source);
    }
  }

  private SourceDataset initializeSource(DataSource source) {
    if (source == null) {
      return null;
    }
    SourceDataset dataset = new SourceDataset(this, source);
    datasets.put(dataset.getName(), dataset);
    return dataset;
  }

  public synchronized SourceDataset addOrUpdateSource
      (@NonNull String name, @NonNull DataSourceImplementation datasource,
          @NonNull ErrorCollector errors) {
    return addOrUpdateSource(DataSourceUpdate.builder().name(name).source(datasource).build(),
        errors);
  }


  public synchronized SourceDataset addOrUpdateSource
      (@NonNull DataSourceUpdate update,
          @NonNull ErrorCollector errors) {
    if (!update.initialize(errors)) {
      return null;
    }
    DataSource source = new DataSource(update);
    errors = errors.resolve(source.getName());

    SourceDataset dataset = datasets.get(source.getName());
    if (dataset == null) {
      dataset = initializeSource(source);
    } else {
      //TODO: should we support updates?
      errors.fatal("Data source with given name [%s] already exists. " +
          "To update existing source, remove and then add", source.getName());
      return null;
    }
    persistence.putDataset(source.getName(), source);

    Set<Name> tableNames = new HashSet<>();
    for (SourceTableConfiguration tbl : update.getTables()) {
      if (Name.validName(tbl.getName())) {
        tableNames.add(dataset.getCanonicalizer().name(tbl.getName()));
      }
    }

    List<SourceTableConfiguration> allTables = new ArrayList<>(update.getTables());
    if (update.isDiscoverTables()) {
      for (SourceTableConfiguration tbl : source.getImplementation()
          .discoverTables(source.getConfig(), errors)) {
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

  public synchronized Pair<SourceDataset, Collection<SourceTable>> removeSource(
      @NonNull Name name) {
    SourceDataset source = datasets.remove(name);
    if (source != null) {
      persistence.removeDataset(name);
      List<SourceTable> tables = new ArrayList<>(source.getTables());
      for (SourceTable tbl : tables) {
        source.removeTable(tbl.getName());
      }
      return Pair.of(source, tables);
    } else {
      return null;
    }
  }

  public Pair<SourceDataset, Collection<SourceTable>> removeSource(@NonNull String name) {
    return Name.getIfValidSystemName(name, this::removeSource);
  }

  public SourceDataset getDataset(@NonNull Name name) {
    return datasets.get(name);
  }

  public SourceDataset getDataset(@NonNull String name) {
    return datasets.get(Name.system(name));
  }

  public Collection<SourceDataset> getDatasets() {
    return datasets.values();
  }

  @Override
  public void close() throws IOException {
  }

}
