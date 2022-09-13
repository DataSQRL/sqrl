package org.apache.calcite.sql.validate;

import ai.datasqrl.plan.local.transpile.TablePath;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import java.util.List;

public interface ExpandableTableNamespace {

  TablePath createTablePath(String alias);

  SQRLTable getDestinationTable();

  List<Relationship> getRelationships();
}
