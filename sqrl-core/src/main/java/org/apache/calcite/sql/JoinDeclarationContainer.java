package org.apache.calcite.sql;

import ai.datasqrl.schema.Relationship;

public interface JoinDeclarationContainer {
    JoinDeclaration getDeclaration(Relationship rel);

    void add(Relationship rel, JoinDeclaration declaration);
  }