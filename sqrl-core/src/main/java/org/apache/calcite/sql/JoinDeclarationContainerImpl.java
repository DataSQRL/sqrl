package org.apache.calcite.sql;

import ai.datasqrl.schema.Relationship;
import java.util.HashMap;
import java.util.Map;

public class JoinDeclarationContainerImpl implements JoinDeclarationContainer {
    Map<Relationship, JoinDeclaration> map = new HashMap<>();
    @Override
    public JoinDeclaration getDeclaration(Relationship rel) {
      return map.get(rel);
    }

    @Override
    public void add(Relationship rel, JoinDeclaration declaration) {
      map.put(rel, declaration);
    }
  }