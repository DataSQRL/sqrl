package org.apache.calcite.sql;

import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ScriptTable;
import java.util.Optional;

public interface TablePath {
    ScriptTable getBaseTable();
    Optional<String> getBaseAlias();

    boolean isRelative();

    int size();

    Relationship getRelationship(int i);

    String getAlias();
  }