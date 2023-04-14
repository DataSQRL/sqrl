package com.datasqrl.module;

import com.datasqrl.canonicalizer.Name;
import org.apache.calcite.schema.Table;

/**
 * Represents a {@link NamespaceObject} for a table.
 */
public interface TableNamespaceObject<T> extends NamespaceObject {

  /**
   * Returns the name of the table.
   *
   * @return the name of the table
   */
  Name getName();

  /**
   * Returns the {@link Table} of the table.
   *
   * @return the {@link Table} of the table
   */
  T getTable();
}
