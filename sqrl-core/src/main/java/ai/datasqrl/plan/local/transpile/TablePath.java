package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import java.util.Optional;

public interface TablePath {

  SQRLTable getBaseTable();

  Optional<String> getBaseAlias();

  boolean isRelative();

  int size();

  Relationship getRelationship(int i);

  String getAlias();
}