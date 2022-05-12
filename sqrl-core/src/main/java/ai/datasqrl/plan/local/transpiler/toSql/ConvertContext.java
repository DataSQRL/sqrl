package ai.datasqrl.plan.local.transpiler.toSql;

import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import java.util.HashMap;
import java.util.Map;
import lombok.Value;

@Value
public class ConvertContext {
  public final AliasGenerator aliasGenerator = new AliasGenerator();
  public final Map<RelationNorm, String> aliasMap = new HashMap<>();
  public boolean isNested;

  public ConvertContext() {
    this(false);
  }
  public ConvertContext(boolean isNested) {
    this.isNested = isNested;
  }

  public String getOrCreateAlias(RelationNorm node) {
    if (this.aliasMap.containsKey(node)) {
      return this.aliasMap.get(node);
    }
    String alias = aliasGenerator.nextTableAliasName().getCanonical();
    this.aliasMap.put(node, alias);

    return alias;
  }

  public ConvertContext nested() {
    return new ConvertContext(true);
  }
}
