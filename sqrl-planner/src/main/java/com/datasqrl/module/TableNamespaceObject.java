package com.datasqrl.module;

import org.apache.calcite.schema.Table;

import com.datasqrl.canonicalizer.Name;

/**
 * Represents a {@link NamespaceObject} for a table.
 */
public interface TableNamespaceObject<T> extends NamespaceObject {

  /**
   * Returns the name of the table.
   *
   * @return the name of the table
   */
  @Override
  Name getName();

  /**
   * Returns the {@link Table} of the table.
   *
   * @return the {@link Table} of the table
   */
  T getTable();
}
