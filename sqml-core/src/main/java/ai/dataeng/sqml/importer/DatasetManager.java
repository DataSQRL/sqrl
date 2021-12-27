package ai.dataeng.sqml.importer;

import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.tree.name.Name;

public interface DatasetManager {

  void register();

  Dataset getDataset(Name first);
}
