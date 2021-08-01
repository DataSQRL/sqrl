package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Literal;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class Analysis {
  private final Script script;
  private Map<Expression, SqmlType> typeMap = new HashMap<>();

  public Analysis(Script script) {
    this.script = script;
  }

  public SqmlType getType(Expression expression) {
    SqmlType type = typeMap.get(expression);
    if (type != null) {
      return type;
    }
    return new UnknownSqmlType();
  }

  public void setType(Expression expression, SqmlType type) {
    typeMap.put(expression, type);
  }

  public List<ResolvedField> getFields(Node select) {
    return ImmutableList.of(
        new ResolvedField("testing_a", new StringSqmlType(), false),
        new ResolvedField("testing_b", new StringSqmlType(), false)
    );
  }

  public QualifiedName getName(QualifiedName name) {
    if (name.getParts().get(0).equals("@")) {
      return QualifiedName.of("a", Math.abs(new Random().nextInt()) + "");
    } else {
      return name;
    }
  }

  public Optional<Boolean> isNonNull(Expression expression) {
    return Optional.empty();
  }
}
