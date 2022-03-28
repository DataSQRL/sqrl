package ai.dataeng.sqml.parser.sqrl.operations;

import ai.dataeng.sqml.planner.LogicalPlanDag;
import ai.dataeng.sqml.tree.name.Name;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

/**
 * Adds datasets with their schema to the namespace.
 */
@AllArgsConstructor
@Getter
public class DatasetImportOperation extends ImportOperation {
  LogicalPlanDag dag;
}
