package ai.dataeng.sqml.schema;

import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.DatasetOrTable;
import ai.dataeng.sqml.planner.SchemaImpl;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.VersionedName;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;

/**
 * Similar to a namespace in code, it helps provide scope for certain identifiers.
 */
public interface Namespace {

  /**
   * Finds the schema objects associated with the name. This can qualify
   *  table lookups to find which dataset they originate from.
   */
  Optional<Table> lookup(NamePath name);
  Optional<Table> lookup(VersionedName name);

  /**
   * Local scoped objects are accessible through its fully qualified path
   *  but are not exported to outside the script. Examples are: Functions
   *  and dataset imports.
   */
  void scope(Dataset dataset);

  void addDataset(Dataset dataset);

  Table createTable(Name name, NamePath path, boolean isInternal);

  /**
   * The current schema
   */
  ShadowingContainer<DatasetOrTable> getSchema();

  SchemaImpl getLogicalPlan();

  void addToDag(SqlNode sqlNode);
}
