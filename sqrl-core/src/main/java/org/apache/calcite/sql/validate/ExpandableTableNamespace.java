package org.apache.calcite.sql.validate;

import ai.datasqrl.plan.local.transpile.TablePath;

public interface ExpandableTableNamespace {

  TablePath createTablePath(String alias);
}
