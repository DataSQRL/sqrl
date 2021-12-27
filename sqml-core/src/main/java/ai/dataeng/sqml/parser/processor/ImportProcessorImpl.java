package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.importer.DatasetManager;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

public class ImportProcessorImpl implements ImportProcessor {

  private final DatasetManager datasetManager;
  private final HeuristicPlannerProvider plannerProvider;

  public ImportProcessorImpl(DatasetManager datasetManager,
      HeuristicPlannerProvider plannerProvider) {
    this.datasetManager = datasetManager;
    this.plannerProvider = plannerProvider;
  }

  @Override
  @SneakyThrows
  public void process(ImportDefinition statement, Namespace namespace) {
    NamePath name = statement.getNamePath();
    Dataset dataset = datasetManager.getDataset(name.getFirst());
    if (name.getLength() > 2) {
      throw new RuntimeException(String.format("Cannot import identifier: %s", name));
    }
    if (name.getLength() == 1) {
      namespace.scope(
          statement.getAliasName()
              .orElse(name.getFirst()),
          dataset);

    } else if (name.get(1).getDisplay().equals("*")) {
      if (statement.getAliasName().isPresent()) {
        throw new RuntimeException(String.format("Could not alias star (*) import: %s", name));
      }
      namespace.addRootDataset(dataset, name.getFirst());

    } else {
      Optional<Table> tableOptional = dataset.get(name.get(1));
      if (tableOptional.isEmpty()) {
        throw new RuntimeException(String.format("Could not find table: %s", name));
      }
      Table table = tableOptional.get();
      List<Table> newTables = new ArrayList<>();
      newTables.add(table);
      Dataset newDataset = new Dataset(dataset.name, newTables);
      Name newName = statement.getAliasName().orElse(name.getFirst());
      namespace.addRootDataset(newDataset, newName);
    }

    plannerProvider.createPlanner().plan(Optional.empty(), namespace, "SELECT * FROM orders");
  }
}