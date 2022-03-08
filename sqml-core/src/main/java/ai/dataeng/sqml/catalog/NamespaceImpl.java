package ai.dataeng.sqml.catalog;


import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.DatasetOrTable;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.operator.DocumentSource;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.planner.operator2.SqrlRelNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.ToString;

@ToString
public class NamespaceImpl implements Namespace {

  private final Map<Name, Dataset> rootDatasets;
  private final Map<Name, Dataset> scopedDatasets;
  private final List<DocumentSource> sources;

  private final static AtomicInteger tableIdCounter = new AtomicInteger(0);
  private final LogicalPlanImpl logicalPlan;

  public NamespaceImpl() {
    this.rootDatasets = new HashMap<>();
    this.scopedDatasets = new HashMap<>();
    this.sources = new ArrayList<>();
    this.logicalPlan = new LogicalPlanImpl();
  }

  @Override
  public Optional<Table> lookup(NamePath namePath) {
    if (namePath.isEmpty()) return Optional.empty();

    //Hack due to order or adding dataset
    //Always look in schema first to discover
    Table schemaTable = (Table)this.logicalPlan.schema.getByName(namePath.getFirst());
    if (schemaTable != null) {
      if (namePath.getLength() == 1) {
        return Optional.of(schemaTable);
      }

      return schemaTable.walk(namePath.popFirst());
    }

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
  public void scope(Dataset dataset) {

  }

  public void addRootDataset(Dataset dataset, Name datasetName) {
    Dataset datasetSchema = rootDatasets.get(datasetName);
    if (datasetSchema != null) {
      datasetSchema.merge(dataset);
    } else {
      this.rootDatasets.put(datasetName, dataset);
    }
  }

  @Override
  public void addDataset(Dataset dataset) {
    Dataset datasetSchema = rootDatasets.get(dataset.getName());
    if (datasetSchema != null) {
      datasetSchema.merge(dataset);
    } else {
      this.rootDatasets.put(dataset.getName(), dataset);
    }
  }

  @Override
  public void addSourceNode(DocumentSource source) {
    logicalPlan.sourceNodes.add(source);
  }

  @Override
  public List<DocumentSource> getSources() {
    return sources;
  }

  @Override
  public Table createTable(Name name, NamePath path, boolean isInternal) {
    Table table = new Table(tableIdCounter.incrementAndGet(), name, path, isInternal);
    logicalPlan.schema.add(table);
    return table;
  }

  @Override
  public Table createTable(Name name, NamePath path, SqrlRelNode node, boolean isInternal) {
    Table table = new Table(tableIdCounter.incrementAndGet(), name, path,isInternal);
    logicalPlan.schema.add(table);
    node.getFields().forEach(table::addField);

    return table;
  }

  @Override
  public ShadowingContainer<DatasetOrTable> getSchema() {
    ShadowingContainer<DatasetOrTable> shadowingContainer = new ShadowingContainer<>();
    for (Dataset dataset : this.rootDatasets.values()) {
      dataset.tables.forEach(shadowingContainer::add);
    }
    return shadowingContainer;
  }

  @Override
  public LogicalPlanImpl getLogicalPlan() {
    return this.logicalPlan;
  }
}
