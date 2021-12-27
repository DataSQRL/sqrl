package ai.dataeng.sqml.catalog;


import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;

@ToString
public class NamespaceImpl implements Namespace {

  private final Map<Name, Dataset> rootDatasets;
  private final Map<Name, Dataset> scopedDatasets;

  public NamespaceImpl() {
    this.rootDatasets = new HashMap<>();
    this.scopedDatasets = new HashMap<>();
  }

  @Override
  public Optional<Table> lookup(NamePath namePath) {
    if (namePath.isEmpty()) return Optional.empty();

    //Check if path is qualified
    Dataset dataset = this.rootDatasets.get(namePath.getFirst());
    if (dataset != null) {
      return dataset.walk(namePath.popFirst());
    }

    //look for table in all root datasets
    for (Map.Entry<Name, Dataset> rootDataset : rootDatasets.entrySet()) {
      Optional<Table> ds = rootDataset.getValue().walk(namePath);
      if (ds.isPresent()) {
        return ds;
      }
    }

    Dataset localDs = scopedDatasets.get(namePath.getFirst());
    if (localDs != null) {
      return localDs.walk(namePath.popFirst());
    }

    return Optional.empty();
  }

  @Override
  public Optional<Table> lookup(NamePath name, int version) {
    return Optional.empty();
  }

  @Override
  public Optional<List<Table>> lookupAll(NamePath name) {
    return Optional.empty();
  }

  @Override
  public void scope(Name name, Dataset dataset) {

  }

  public void addRootDataset(Dataset dataset, Name datasetName) {
    Dataset datasetSchema = rootDatasets.get(datasetName);
    if (datasetSchema != null) {
      datasetSchema.merge(dataset);
    } else {
      this.rootDatasets.put(datasetName, dataset);
    }
  }
}
