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
import java.util.List;
import java.util.Optional;

/**
 * Similar to a namespace in code, it helps provide scope for certain identifiers.
 */
public interface Namespace {

  /**
   * Finds the schema objects associated with the name. This can qualify
   *  table lookups to find which dataset they originate from.
   */
  Optional<Table> lookup(NamePath name);

  /**
   * Finds a specific version of a schema object
   */
  Optional<Table> lookup(NamePath name, int version);

  /**
   * Local scoped objects are accessible through its fully qualified path
   *  but are not exported to outside the script. Examples are: Functions
   *  and dataset imports.
   */
  void scope(Dataset dataset);

  void addDataset(Dataset dataset);

  void addSourceNode(DocumentSource source);

  List<DocumentSource> getSources();

  Table createTable(Name name, NamePath path, boolean isInternal);

  /**
   * The current schema
   */
  ShadowingContainer<DatasetOrTable> getSchema();

  LogicalPlanImpl getLogicalPlan();

  Table createTable(Name last, NamePath namePath, SqrlRelNode node, boolean isInternal);
}
