package org.apache.calcite.jdbc;


import org.apache.calcite.schema.Table;

public interface TableResolver {
  Table resolve(String path);
}
