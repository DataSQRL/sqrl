package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.schema.Relationship;

public interface JoinDeclarationContainer {
    JoinDeclaration getDeclaration(Relationship rel);

    void add(Relationship rel, JoinDeclaration declaration);
  }