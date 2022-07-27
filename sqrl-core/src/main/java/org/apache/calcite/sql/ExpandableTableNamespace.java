package org.apache.calcite.sql;

public interface ExpandableTableNamespace {

  TablePath createTablePath(String alias);
}
