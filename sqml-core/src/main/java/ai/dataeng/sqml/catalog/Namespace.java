package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.operator.DocumentSource;
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
  public Optional<Table> lookup(NamePath name);

  /**
   * Finds a specific version of a schema object
   */
  public Optional<Table> lookup(NamePath name, int version);

  /**
   * Returns all objects associated with
   */
  public Optional<List<Table>> lookupAll(NamePath name);

  /**
   * Local scoped objects are accessible through its fully qualified path
   *  but are not exported to outside the script. Examples are: Functions
   *  and dataset imports.
   */
  public void scope(Dataset dataset);

  void addDataset(Dataset dataset);

  void addSourceNode(DocumentSource source);

  Table createTable(Name name, boolean isInternal);

  /**
   * The current schema
   */
//  public Table getRootTable();
}
