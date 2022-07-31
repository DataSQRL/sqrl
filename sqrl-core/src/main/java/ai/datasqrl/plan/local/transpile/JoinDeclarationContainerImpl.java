package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.schema.Relationship;
import java.util.HashMap;
import java.util.Map;

public class JoinDeclarationContainerImpl implements JoinDeclarationContainer {
    Map<Relationship, SqlJoinDeclaration> map = new HashMap<>();
    @Override
    public SqlJoinDeclaration getDeclaration(Relationship rel) {
      return map.get(rel);
    }

    @Override
    public void add(Relationship rel, SqlJoinDeclaration declaration) {
      map.put(rel, declaration);
    }
  }