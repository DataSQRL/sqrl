package ai.dataeng.sqml.parser.sqrl.analyzer;


import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.and;
import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.getCriteria;
import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.getRelation;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.sqrl.PathUtil;
import ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.JoinBuilder;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.function.RewritingFunction;
import ai.dataeng.sqml.parser.sqrl.function.SqrlFunction;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExpressionAnalyzer {
  private final JoinBuilder joinBuilder;
  FunctionLookup functionLookup = new FunctionLookup();
  AliasGenerator gen = new AliasGenerator();

  public ExpressionAnalyzer(
      JoinBuilder joinBuilder) {
    this.joinBuilder = joinBuilder;
  }

  public Expression analyze(Expression node, Scope scope) {
    Visitor visitor = new Visitor();
    Expression newExpression =
        ExpressionTreeRewriter.rewriteWith(visitor, node, new Context(scope));

    return newExpression;
  }

  public static class Context {
    private final Scope scope;

    public Context(Scope scope) {
      this.scope = scope;
    }

    public Scope getScope() {
      return scope;
    }
  }

  class Visitor extends ExpressionRewriter<Context> {
    @Override
    public Expression rewriteFunctionCall(FunctionCall node, Context context,
        ExpressionTreeRewriter<Context> treeRewriter) {
      SqrlFunction function = functionLookup.lookup(node.getName());
      if (function instanceof RewritingFunction) {
        //rewrite function immediately
        RewritingFunction rewritingFunction = (RewritingFunction) function;
        node = rewritingFunction.rewrite(node);
      }

      List<Expression> arguments = new ArrayList<>();

      for (Expression arg : node.getArguments()) {
        arguments.add(treeRewriter.rewrite(arg, context));
      }

      //Todo: allow expanding aggregates more than a single argument
      if (function.isAggregate() && node.getArguments().size() == 1 &&
          node.getArguments().get(0) instanceof Identifier) {
        Identifier identifier = (Identifier)node.getArguments().get(0);
        FieldPath fieldPath = context.getScope().resolveField(identifier.getNamePath())
            .get(0);
        if (PathUtil.isToMany(fieldPath)) {
          String tableAlias = gen.nextTableAlias();
          String fieldAlias = gen.nextAlias();
          //For cases like COUNT(rel), expand to reference primary key
          if (fieldPath.getLastField() instanceof Relationship) {
            List<Field> fields = new ArrayList<>(fieldPath.getFields());
            fields.add(((Relationship) fieldPath.getLastField()).getToTable().getPrimaryKeys().get(0));
            fieldPath = new FieldPath(fields);
          }

          Name sourceTableAlias = context.getScope().qualify(identifier.getNamePath()).get(0);
          extractFunctionToSubquery(node, fieldPath, tableAlias, fieldAlias, sourceTableAlias, context);
          return new Identifier(Optional.empty(), NamePath.of(tableAlias, fieldAlias));
        }
      }

      return new FunctionCall(node.getLocation(), node.getName(), arguments,
          node.isDistinct(), node.getOver());
    }

    /**
     * Moves a to-many aggregate function into a subquery
     *
     * SELECT __t4.__f5 FROM  Orders AS _
     * LEFT JOIN (SELECT _uuid, count(entries) AS __f5 FROM Orders GROUP BY _uuid) AS __t4
     * 1. need to alias first field
     *
     */
    private Relation extractFunctionToSubquery(FunctionCall node, final FieldPath fieldPath,
        String tableAlias, String fieldAlias, Name sourceTableAlias,
        Context context) {
      NamePath tableAliasName = NamePath.of(tableAlias);

      //We extract the primary keys of the table we came from
      Field first = fieldPath.getFields().get(0);
      Preconditions.checkState(first instanceof Relationship, "Pathed identifiers must be columns");
      Relationship rel = (Relationship) first;
      Name currentAlias = gen.nextTableAliasName();

      List<SelectItem> columns = new ArrayList<>();
      List<Expression> keys = toExpression(currentAlias, rel.getTable().getPrimaryKeys());

      //Create grouping keys, alias for joining on outer query
      Map<Name, Column> criteriaMap = new LinkedHashMap<>();
      for (int i = 0; i < keys.size(); i++) {
        Expression ex = keys.get(i);
        Name alias = gen.nextAliasName();
        columns.add(new SingleColumn(ex, new Identifier(Optional.empty(), alias.toNamePath())));
        criteriaMap.put(alias, rel.getTable().getPrimaryKeys().get(i));
      }

      //Construct inner query relations
      JoinBuilder subqueryJoinBuilder = new JoinBuilder();
      subqueryJoinBuilder.add(Type.INNER, Optional.empty(), rel, NamePath.of(currentAlias), null);

      for (int i = 1; i < fieldPath.getFields().size() - 1; i++) {
        Relationship joinRel = (Relationship)  fieldPath.getFields().get(i);
        currentAlias = gen.nextTableAliasName();

        subqueryJoinBuilder.add(Type.INNER, Optional.empty(), joinRel, NamePath.of(currentAlias), null);
      }

      //Rewrite aggregate and add node to select list
      Expression rewrittenNode = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>() {
        @Override
        public Expression rewriteIdentifier(Identifier node, Name context,
            ExpressionTreeRewriter<Name> treeRewriter) {

          return new Identifier(Optional.empty(),
              NamePath.of(context).concat(fieldPath.getLastField().getId()));
        }
      }, node, currentAlias);
      columns.add(new SingleColumn(Optional.empty(), rewrittenNode, Optional.of(new Identifier(Optional.empty(), NamePath.of(fieldAlias)))));

      //Build subquery tokens
      QuerySpecification spec = new QuerySpecification(
          Optional.empty(),
          new Select(Optional.empty(), false, columns),
          subqueryJoinBuilder.build(),
          Optional.empty(),
          Optional.of(new GroupBy(new SimpleGroupBy(keys))),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
      );
      Query query = new Query(spec, Optional.empty(), Optional.empty());
      AliasedRelation aliasedRelation = new AliasedRelation(Optional.empty(), new TableSubquery(Optional.empty(), query),
          new Identifier(Optional.empty(), tableAliasName));


      //Build criteria
      List<Expression> expressions = new ArrayList<>();
      for (Map.Entry<Name, Column> entry : criteriaMap.entrySet()) {
          Identifier lhs = new Identifier(Optional.empty(), tableAliasName.concat(entry.getValue().getId()));
          //We assume that the column names are the same
          Identifier rhs = new Identifier(Optional.empty(), sourceTableAlias.toNamePath().concat(entry.getValue().getId()));
          expressions.add(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, lhs, rhs));
      }
      Expression criteria = and(expressions);
      JoinOn join = new JoinOn(Optional.empty(), criteria);
      //Assemble outer join to subquery
      joinBuilder.add(Type.LEFT, Optional.of(join), fieldPath.getLastField(), tableAliasName, aliasedRelation);

      return aliasedRelation;
    }

    private List<Expression> toExpression(Name tableAlias, List<Column> columns) {
      return columns.stream()
          .map(e->new Identifier(Optional.empty(), NamePath.of(tableAlias, e.getName())))
          .collect(Collectors.toList());
    }

    @Override
    public Expression rewriteIdentifier(Identifier node, Context context,
        ExpressionTreeRewriter<Context> treeRewriter) {
      List<FieldPath> resolved = context.getScope()
          .resolveField(node.getNamePath());
      if (resolved.size() != 1) {
        System.out.println();
        context.getScope()
            .resolveField(node.getNamePath());
      }

      Preconditions.checkState(resolved.size() == 1,
          "Could not resolve field (ambiguous or non-existent: " + node + " : " + resolved + ")");

      if (PathUtil.isToOne(resolved.get(0))) {
        Name fieldName = resolved.get(0).getLastField().getId();
        Name tableAlias = gen.nextTableAliasName();
        Name sourceTableAlias = context.getScope().qualify(node.getNamePath()).get(0);
        addLeftJoin(node, resolved.get(0), tableAlias, sourceTableAlias);
        return new Identifier(node.getLocation(), tableAlias.toNamePath().concat(fieldName));
      }

      NamePath qualifiedName = context.getScope().qualify(node.getNamePath());
      return new Identifier(node.getLocation(), qualifiedName);
    }

    private void addLeftJoin(Identifier node, FieldPath fieldPath,
        Name tableAlias, Name sourceTableAlias) {

      Relationship rel = (Relationship) fieldPath.getFields().get(0);

      joinBuilder.add(Type.LEFT, getCriteria(rel, sourceTableAlias, tableAlias),
          fieldPath.getFields().get(0), tableAlias.toNamePath(),
          getRelation(rel, tableAlias));
    }
  }
}
