package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.schema.Relationship;

public interface JoinDeclarationContainer {
    SqlJoinDeclaration getDeclaration(Relationship rel);

    void add(Relationship rel, SqlJoinDeclaration declaration);
  }