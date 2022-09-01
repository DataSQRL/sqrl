package org.apache.calcite.sql.validate;

import ai.datasqrl.plan.local.transpile.TablePath;
import ai.datasqrl.schema.SQRLTable;

public interface ExpandableTableNamespace {

  TablePath createTablePath(String alias);

  SQRLTable getDestinationTable();
}
