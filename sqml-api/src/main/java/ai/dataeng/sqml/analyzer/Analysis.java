package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.SqmlType;
import java.util.HashMap;
import java.util.Map;

public class Analysis {

  private final Map<QualifiedName, SqmlType> typeMap = new HashMap<>();
  private final Script rewritten;

  public Analysis(Script rewritten) {

    this.rewritten = rewritten;
  }

  public Map<QualifiedName, SqmlType> getTypeMap() {
    return typeMap;
  }

  public Script getRewrittenScript() {
    return rewritten;
  }
}
