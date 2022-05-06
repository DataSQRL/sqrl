package ai.datasqrl.plan.local.transpiler;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;

import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.function.RewritingFunction;
import ai.datasqrl.function.SqlNativeFunction;
import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionRewriter;
import ai.datasqrl.parse.tree.ExpressionTreeRewriter;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.transforms.ExtractSubQuery;
import ai.datasqrl.plan.local.transpiler.util.CriteriaUtil;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.Table;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.RelationshipRef;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableOrRelationship;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import graphql.com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class ExpressionNormalizer extends ExpressionRewriter<RelationScope> {

  private final boolean allowPaths;
  @Getter
  private List<JoinNorm> addlJoins = new ArrayList<>();

  public ExpressionNormalizer(boolean allowPaths) {
    this.allowPaths = allowPaths;
  }

  @Override
  public Expression rewriteIdentifier(Identifier node, RelationScope scope,
      ExpressionTreeRewriter<RelationScope> treeRewriter) {
    NamePath namePath = node.getNamePath();
    //1. lookup in scope to see if we're dealing with an alias, or expression
    //   validate we have an unambiguous result

    List<RelationNorm> table = scope.resolve(namePath);
    if (table.isEmpty()) {
      throw new RuntimeException("Could not find field " + namePath);
    } else if (table.size() > 1) {
      throw new RuntimeException("Ambiguous field " + namePath + " " + table);
    }

    if (table.get(0) instanceof QuerySpecNorm) {
      return new ReferenceExpression(table.get(0), ((QuerySpecNorm)table.get(0)).getField(namePath));
    }

    TableNodeNorm base = (TableNodeNorm)table.get(0);
    //2. If we're dealing with an alias, pop first, resolve remainder of path
    //   Else resolve the entire path
    NamePath path = removeAliasFromPath(namePath, scope);

    //3. If !allowPaths and is path, error and return
    if (path.getLength() > 1 && !allowPaths) {
      throw new RuntimeException("Paths encountered where path is not allowed");
    }
//
//    //4. Walk path, append a left join
    List<TableOrRelationship> toExpand = new ArrayList<>();
    for (int i = 0; i < path.getLength() - 1; i++) {
      Field field = base.getRef().getTable().getField(path.get(i)).get();
      if (field instanceof Relationship) {
        toExpand.add(new RelationshipRef((Relationship) field));
      } else if (i != path.getLength() - 1){
        throw new RuntimeException("field not expected here");
      }
    }
    if (isToManyRef(toExpand)) {
      throw new RuntimeException("To-many relationship not expected here:" + path);
    }

    Field field = base.getRef().getTable().walkField(path).get();

    if (toExpand.size() > 0) {
      JoinNorm expanded = (JoinNorm) TablePathToJoins.expand(toExpand);

      Optional<JoinOn> criteria = CriteriaUtil.joinEq(base, expanded);

      JoinNorm joinNorm = new JoinNorm(Optional.empty(), Type.LEFT, base, expanded, criteria);

      scope.getAddlJoins().add(joinNorm);
      return new ResolvedColumn(node, expanded.getRightmost(), (Column)field);
    }

    //At the end, check if we're at a relationship & expand it or just return column;
    return new ResolvedColumn(node, base, (Column)field);
  }

  private boolean isToManyRef(List<TableOrRelationship> list) {
    return isToMany(list.stream().map(e->((RelationshipRef) e).getRelationship()).collect(Collectors.toList()));
  }

  @Override
  public Expression rewriteFunctionCall(FunctionCall node, RelationScope scope,
      ExpressionTreeRewriter<RelationScope> treeRewriter) {
    FunctionLookup functionLookup = new FunctionLookup();
    SqrlFunction function = functionLookup.lookup(node.getNamePath());

    if (function.isAggregate() && node.getArguments().size() == 1 &&
        node.getArguments().get(0) instanceof Identifier) {
      Identifier identifier = (Identifier) node.getArguments().get(0);
      //Analyze identifier:
      List<RelationNorm> table = scope.resolve(identifier.getNamePath());
      NamePath path = removeAliasFromPath(identifier.getNamePath(), scope);
      List<Field> fields = ((TableNodeNorm)table.get(0)).getRef().getTable().walkFields(path).get();
      if (fields.get(fields.size() - 1) instanceof Relationship) {
        Relationship relationship = (Relationship) fields.get(fields.size() - 1);
        fields.add(relationship.getToTable().getPrimaryKeys().get(0));
      }

      if (fields.size() > 1 && isToMany(fields)) {
        return pushIntoSubQuery(node, scope);
      }
    }

    //Special case for count
    //TODO Replace this bit of code
    if (function instanceof SqlNativeFunction) {
      SqlNativeFunction nativeFunction = (SqlNativeFunction) function;
      if (nativeFunction.getOp().getName().equalsIgnoreCase("COUNT") &&
          node.getArguments().size() == 0) {
        return new ResolvedFunctionCall(new FunctionCall(NamePath.of("COUNT"), List.of(new LongLiteral("1")), false),
            new SqlNativeFunction(SqlStdOperatorTable.COUNT));
      }
    }

    if (function instanceof RewritingFunction) {
      //rewrite function immediately
      RewritingFunction rewritingFunction = (RewritingFunction) function;
      node = rewritingFunction.rewrite(node);
    }
    List<Expression> arguments = new ArrayList<>();
    for (Expression arg : node.getArguments()) {
      arguments.add(treeRewriter.rewrite(arg, scope));
    }

    if (function instanceof SqlNativeFunction
        && ((SqlNativeFunction) function).getOp() instanceof SqlAggFunction
        &&((SqlNativeFunction) function).getOp().requiresOver() &&
        node.getOver().isEmpty()) { //TODO logic here not correct
      throw new RuntimeException("todo over");
//      FunctionCall functionCall = new FunctionCall(node.getLocation(), node.getNamePath(), arguments,
//          node.isDistinct(),
//          new AddWindowToOverFunction().createWindow(scope));
//      scope.getScopes().put(functionCall, new FunctionCallScope(function, true));
//      return functionCall;
    }

    FunctionCall functionCall = new FunctionCall(node.getLocation(), node.getNamePath(), arguments,
        node.isDistinct(), node.getOver());

    return new ResolvedFunctionCall(functionCall, function);
  }

  private boolean isToMany(List<Field> fields) {
    if (fields.isEmpty()) return false;
    for (Field field : fields) {
      if (field instanceof Relationship && ((Relationship) field).getMultiplicity() != Multiplicity.MANY) {
        return false;
      }
    }
    return true;
  }

  /**
   * SELECT count(entries) FROM _;
   *
   * SELECT *
   * FROM entries e
   * LEFT JOIN (SELECT _._uuid, count(e._uuid)
   *             FROM _ JOIN entries e GROUP BY _._uuid WHERE _._uuid = e._uuid) x
   * WHERE x._uuid = e._uuid;
   */
  private Expression pushIntoSubQuery(FunctionCall node, RelationScope scope) {
    Identifier arg = (Identifier) node.getArguments().get(0);
    List<RelationNorm> resolved = scope.resolve(arg.getNamePath());

    List<Field> fields = ((TableNodeNorm)resolved.get(0)).getRef().getTable()
        .walkFields(removeAliasFromPath(arg.getNamePath(), scope)).get();
    if (fields.get(fields.size() - 1) instanceof Relationship) {
      Relationship rel = (Relationship)fields.get(fields.size() - 1);
      fields.add(rel.getToTable().getPrimaryKeys().get(0));
    }

    NamePath relPath = NamePath.of(fields.stream().map(Field::getName).collect(Collectors.toList()));

    TableNodeNorm baseRel = (TableNodeNorm)resolved.get(0);
    Table baseTable = ((TableNodeNorm)resolved.get(0)).getRef().getTable();

    Name fieldName = fields.get(fields.size() - 1).getName();
    NamePath tablePath = Name.SELF_IDENTIFIER.toNamePath().concat(relPath.popLast());
    QuerySpecification spec = ExtractSubQuery.extract(node, fieldName, tablePath);

    RelationScope subQueryScope = new RelationScope(scope.getSchema(),
        Optional.of(baseTable), scope.getIsExpression(), scope.getTargetName());
    RelationNormalizer relationNormalizer = new RelationNormalizer();
    QuerySpecNorm subquery = (QuerySpecNorm)spec.accept(relationNormalizer, subQueryScope);

    Preconditions.checkState(subquery.getParentPrimaryKeys().size() ==
        baseTable.getPrimaryKeys().size(), "Cannot rejoin subquery on primary key");

    Optional<JoinOn> criteria = CriteriaUtil.subqueryEq(baseTable, baseRel, subquery);

    JoinNorm join = new JoinNorm(Optional.empty(), Type.LEFT, baseRel, subquery, criteria);

    this.addlJoins.add(join);

    return new ReferenceExpression(subquery, subquery.getSelect().getSelectItems().get(0).getExpression());
  }

  private NamePath removeAliasFromPath(NamePath namePath, RelationScope scope) {
    if (scope.getJoinScopes().get(namePath.getFirst()) != null
        && scope.getJoinScopes().get(namePath.getFirst()).walk(namePath.popFirst()).isPresent()) {
      return namePath.popFirst();
    } else {
      return namePath;
    }
  }
}
