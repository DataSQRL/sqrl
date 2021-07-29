package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Analysis {

  private final Map<QualifiedName, SqmlType> typeMap = new HashMap<>();
  private final Script rewritten;
  private final Map<Expression, ResolvedField> fieldMap = new HashMap<>();
  private final Set<Expression> unionRules = new HashSet<>();

  public Analysis(Script rewritten) {

    this.rewritten = rewritten;
  }

  public Map<QualifiedName, SqmlType> getTypeMap() {
    return typeMap;
  }

  public Script getRewrittenScript() {
    return rewritten;
  }

  public ResolvedField getResolvedField(Expression expression) {
//    return fieldMap.get(expression);
    return new ResolvedField("test", new StringSqmlType(), false);
  }

  public String getName(Node node) {
    return "name" + Math.abs(new Random().nextInt());
  }

  public List<ResolvedField> getFields(Node select) {
    return ImmutableList.of(
        new ResolvedField("a", new StringSqmlType(), false),
        new ResolvedField("sum_id", new StringSqmlType(), false)
    );
  }

  public boolean followsUnionRules(Union node) {
    return unionRules.contains(node);
  }
}
