package ai.dataeng.sqml.parser.validator;

import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.tree.ExistsPredicate;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.InPredicate;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.QuantifiedComparisonExpression;
import ai.dataeng.sqml.tree.SubqueryExpression;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

@Getter
public class ExpressionAnalysis {
  private final Map<NodeRef<Expression>, Type> expressionTypes;
  private final Map<NodeRef<Expression>, Type> expressionCoercions;
  private final Set<NodeRef<Expression>> typeOnlyCoercions;
  private final Map<NodeRef<Expression>, FieldId> columnReferences;
  private final Set<NodeRef<InPredicate>> subqueryInPredicates;
  private final Set<NodeRef<SubqueryExpression>> scalarSubqueries;
  private final Set<NodeRef<ExistsPredicate>> existsSubqueries;
  private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons;
//  private final Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = new HashMap<>();

  // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
//  private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences;
//  private final Set<NodeRef<FunctionCall>> windowFunctions;

  public ExpressionAnalysis()
  {
    this.expressionTypes = new HashMap<>();
    this.expressionCoercions = new HashMap<>();
    this.typeOnlyCoercions = new HashSet<>();
    this.columnReferences = new HashMap<>();
    this.subqueryInPredicates = new HashSet<>();
    this.scalarSubqueries = new HashSet<>();
    this.existsSubqueries = new HashSet<>();
    this.quantifiedComparisons = new HashSet<>();
//    this.lambdaArgumentReferences = ImmutableMap.copyOf(requireNonNull(lambdaArgumentReferences, "lambdaArgumentReferences is null"));
//    this.windowFunctions = ImmutableSet.copyOf(requireNonNull(windowFunctions, "windowFunctions is null"));
  }

  public Type getType(Expression expression)
  {
    return expressionTypes.get(NodeRef.of(expression));
  }

  public Map<NodeRef<Expression>, Type> getExpressionTypes()
  {
    return expressionTypes;
  }

  public Type getCoercion(Expression expression)
  {
    return expressionCoercions.get(NodeRef.of(expression));
  }

  public boolean isTypeOnlyCoercion(Expression expression)
  {
    return typeOnlyCoercions.contains(NodeRef.of(expression));
  }

  public boolean isColumnReference(Expression node)
  {
    return columnReferences.containsKey(NodeRef.of(node));
  }

  public Set<NodeRef<InPredicate>> getSubqueryInPredicates()
  {
    return subqueryInPredicates;
  }

  public Set<NodeRef<SubqueryExpression>> getScalarSubqueries()
  {
    return scalarSubqueries;
  }

  public Set<NodeRef<ExistsPredicate>> getExistsSubqueries()
  {
    return existsSubqueries;
  }

  public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
  {
    return quantifiedComparisons;
  }

  public void putExpressionType(NodeRef<Expression> node, Type type) {
    expressionTypes.put(node, type);
  }

//  public Set<NodeRef<FunctionCall>> getWindowFunctions()
//  {
//    return windowFunctions;
//  }

}
