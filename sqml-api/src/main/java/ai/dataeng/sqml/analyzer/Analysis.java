package ai.dataeng.sqml.analyzer;

import static com.google.common.collect.Multimaps.forMap;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.schema.SchemaProvider;
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
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
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
  private Map<QualifiedName, SqmlType> types = new HashMap<>();
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
      throw new RuntimeException(String.format("Expressiont not named: %s %s",
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

  public Optional<RelationSqmlType> getRelation(QualifiedName name) {
    SqmlType type = types.get(name);
    if (!(type instanceof RelationSqmlType)) {
      return Optional.empty();
    }

    return Optional.of((RelationSqmlType) type);
  }
//
//  public void addType(QualifiedName name, RelationSqmlType type) {
//    types.put(name, type);
//    name.getPrefix().ifPresent(p->{
//      RelationSqmlType rel = getOrCreateRelation(p);
//
//      rel.setField(Field.newUnqualified(name.getSuffix(), type));
//    });
//  }

  public RelationSqmlType getOrCreateRelation(QualifiedName name) {
    SqmlType type = types.get(name);
    if (type == null) {
      RelationSqmlType relationSqmlType = new RelationSqmlType();
      types.put(name, relationSqmlType);
      return relationSqmlType;
    }
    if (!(type instanceof RelationSqmlType)) {
      throw new RuntimeException(String.format("%s not a relation type", name));
    }

    return (RelationSqmlType)type;

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
