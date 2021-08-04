package ai.dataeng.sqml.analyzer;

import static com.google.common.collect.Multimaps.forMap;

import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public class Analysis {
  private final Script script;
  private Map<Expression, SqmlType> typeMap = new HashMap<>();
  private final Map<Node, Scope> scopes = new LinkedHashMap<>();
  private Map<Node, List<Expression>> outputExpressions = new HashMap<>();
  private final Multimap<NodeRef<Expression>, FieldId> columnReferences = ArrayListMultimap.create();
  private Map<Expression, String> nameMap = new HashMap<>();

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

  public void addName(Expression expression, Optional<Identifier> alias) {
    if (alias.isPresent()) {
      nameMap.put(expression, alias.get().toString());
    } else if (expression instanceof Identifier) {
      nameMap.put(expression, ((Identifier)expression).getValue());
    } else {
      throw new RuntimeException(String.format("Expression not named: %s %s",
          expression.getClass().getName(), expression));
    }
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

  public void setScope(Node node, Scope scope) {
    scopes.put(node, scope);
  }

  public void setOutputExpressions(Node node, List<Expression> expressions) {
    outputExpressions.put(node, expressions);
  }

  public void addTypes(Map<Expression, SqmlType> expressionTypes) {
    this.typeMap.putAll(expressionTypes);
  }

  public void addColumnReferences(Map<NodeRef<Expression>, FieldId> columnReferences)
  {
    this.columnReferences.putAll(forMap(columnReferences));
  }

  public void addColumnReference(NodeRef<Expression> node, FieldId fieldId)
  {
    this.columnReferences.put(node, fieldId);
  }

  public Multimap<NodeRef<Expression>, FieldId> getColumnReferences() {
    return columnReferences;
  }

  public void setGroupByExpressions(QuerySpecification node, List<Expression> groupingExpressions) {

  }

  public void setGroupingSets(QuerySpecification node, List<List<Set<Object>>> sets) {

  }

  public List<Expression> getOutputExpressions(Node node) {
    return outputExpressions.get(node);
  }

  public String getName(Expression expression) {
    return nameMap.get(expression);

  }

  public void setJoinCriteria(Join node, Expression expression) {

  }

}
