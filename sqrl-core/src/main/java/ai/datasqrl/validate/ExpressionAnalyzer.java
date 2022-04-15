package ai.datasqrl.validate;


import static ai.datasqrl.transform.JoinWalker.createTableCriteria;
import static ai.datasqrl.transform.JoinWalker.expandRelation;
import static ai.datasqrl.parse.util.SqrlNodeUtil.alias;
import static ai.datasqrl.parse.util.SqrlNodeUtil.function;
import static ai.datasqrl.parse.util.SqrlNodeUtil.ident;
import static ai.datasqrl.parse.util.SqrlNodeUtil.selectAlias;

import ai.datasqrl.util.AliasGenerator;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.TableFactory;
import ai.datasqrl.transform.JoinWalker;
import ai.datasqrl.transform.JoinWalker.TableItem;
import ai.datasqrl.transform.JoinWalker.WalkResult;
import ai.datasqrl.validate.Scope.ResolveResult;
import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.function.RewritingFunction;
import ai.datasqrl.function.SqlNativeFunction;
import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.transform.Transformers;
import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionRewriter;
import ai.datasqrl.parse.tree.ExpressionTreeRewriter;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinCriteria;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlAggFunction;

public class ExpressionAnalyzer {
  FunctionLookup functionLookup = new FunctionLookup();
  AliasGenerator gen = new AliasGenerator();
  public List<JoinResult> joinResults = new ArrayList<>();

  public ExpressionAnalyzer() {
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
      //Special case for count
      //TODO Replace this bit of code
      if (function instanceof SqlNativeFunction) {
        SqlNativeFunction nativeFunction = (SqlNativeFunction) function;
        if (nativeFunction.getOp().getName().equalsIgnoreCase("COUNT") &&
         node.getArguments().size() == 0
        ) {
          return new FunctionCall(NamePath.of("COUNT"), List.of(new LongLiteral("1")), false);
        }
      }

      if (function instanceof RewritingFunction) {
        //rewrite function immediately
        RewritingFunction rewritingFunction = (RewritingFunction) function;
        node = rewritingFunction.rewrite(node);
      }

      List<Expression> arguments = new ArrayList<>();

      for (Expression arg : node.getArguments()) {
        arguments.add(treeRewriter.rewrite(arg, context));
      }

      if (function instanceof SqlNativeFunction && ((SqlNativeFunction)function).getOp() instanceof SqlAggFunction &&
          ((SqlAggFunction)((SqlNativeFunction)function).getOp()).requiresOver()) {
        return new FunctionCall(node.getLocation(), node.getName(), arguments, node.isDistinct(),
            createWindow(context.getScope()));
      }

      if (function.isAggregate() && node.getArguments().size() == 1 &&
          node.getArguments().get(0) instanceof Identifier) {
        Identifier identifier = (Identifier)node.getArguments().get(0);
        /*
         * The first token is either the join scope or a column in any join scope
         */
        List<ResolveResult> resolve = context.getScope().resolveFirst(identifier.getNamePath());
        Preconditions.checkState(resolve.size() == 1,
            "Column ambiguous or missing: %s %s", identifier.getNamePath(), resolve);
        if(PathUtil.isToMany(resolve.get(0))) {
          ResolveResult result = resolve.get(0);

          JoinWalker joinWalker = new JoinWalker();
          WalkResult walkResult = joinWalker.walk(
              result.getAlias(), //The table alias that resolved the field
              Optional.empty(), //Terminal alias specified
              result.getRemaining().get(), //The fields from the base that we should resolve
              Optional.empty(), //push in relation
              Optional.empty(),
              context.getScope().getJoinScope()
          );
          TableItem first = walkResult.getTableStack().get(0);
          TableItem last = walkResult.getTableStack().get(walkResult.getTableStack().size() - 1);

          //Special case: count(entries) => count(e._uuid) FROM entries
          Field field = result.getTable().walkField(result.getRemaining().get()).get();
          Name fieldName = field instanceof Relationship ?
              ((Relationship) field).getToTable().getPrimaryKeys().get(0).getId()
              : result.getTable().walkField(result.getRemaining().get()).get().getId();

          Name tableAlias = gen.nextTableAliasName();
          Name fieldAlias = gen.nextAliasName();

          //Add context keys to query, rename field
          QuerySpecification subquerySpec = new QuerySpecification(
              Optional.empty(),
              new Select(false, List.of(
                  selectAlias(function(node.getName(), alias(last.getAlias(), fieldName)),
                      fieldAlias.toNamePath()))),
              walkResult.getRelation(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty()
          );

          //Add context keys to query so we can rejoin it in the subsequent step
          QuerySpecification contextSubquery = Transformers.addContextToQuery.transform(subquerySpec, resolve.get(0).getTable(),
              first.getAlias());

          Query query = new Query(Optional.empty(), contextSubquery, Optional.empty(), Optional.empty());
          TableSubquery tableSubquery = new TableSubquery(query);

          TableFactory tableFactory = new TableFactory();
          Table table = tableFactory.create(query);

          context.getScope().getJoinScope().put(tableAlias, table);

          AliasedRelation subquery = new AliasedRelation(tableSubquery, ident(tableAlias));

          JoinCriteria criteria = createTableCriteria(context.getScope().getJoinScope(), result.getAlias(), tableAlias);
          joinResults.add(new JoinResult(Type.LEFT, subquery, Optional.of(criteria)));
          return new Identifier(Optional.empty(), NamePath.of(tableAlias, fieldAlias));
        }
      }

      return new FunctionCall(node.getLocation(), node.getName(), arguments,
          node.isDistinct(), node.getOver());
    }

    private Optional<Window> createWindow(Scope scope) {
      Table table = scope.getFieldScope(Name.SELF_IDENTIFIER).get();
      List<Expression> partition = new ArrayList<>();
      for (Column column : table.getPrimaryKeys()) {
        partition.add(new Identifier(Optional.empty(), Name.SELF_IDENTIFIER.toNamePath().concat(column.getId().toNamePath())));
      }

      OrderBy orderBy = new OrderBy(List.of(
          new SortItem(Optional.empty(),
              new Identifier(Optional.empty(), Name.INGEST_TIME.toNamePath()),
              Optional.empty())));

      return Optional.of(new Window(partition, Optional.of(orderBy)));
    }

    @Override
    public Expression rewriteIdentifier(Identifier node, Context context,
        ExpressionTreeRewriter<Context> treeRewriter) {
      List<Scope.ResolveResult> results = context.getScope().resolveFirst(node.getNamePath());

      Preconditions.checkState(results.size() == 1,
          "Could not resolve field (ambiguous or non-existent: " + node + " : " + results + ")");

      if (PathUtil.isToOne(results.get(0))) {
        ResolveResult result = results.get(0);
        /*
         * Replace current token with an Identifier and expand the path into a subquery. This
         * subquery is joined to the table it was resolved from.
         */
        Name baseTableAlias = gen.nextTableAliasName();
        Relation relation = expandRelation(context.getScope().getJoinScope(), (Relationship)result.getFirstField(), baseTableAlias);

        joinResults.add(new JoinResult(
            Type.LEFT, relation,
            Optional.of(JoinWalker.createRelCriteria(context.getScope().getJoinScope(), result.getAlias(), baseTableAlias,
                (Relationship) result.getFirstField()))
        ));
        return new Identifier(node.getLocation(), baseTableAlias.toNamePath().concat(
            result.getRemaining().get().getLast()));
      }

      NamePath qualifiedName = context.getScope().qualify(node.getNamePath());
      Identifier identifier = new Identifier(node.getLocation(), qualifiedName);
      identifier.setResolved(results.get(0).getFirstField());
      return identifier;
    }
  }

  @Value
  public static class JoinResult {
    Type type;
    Relation relation;
    Optional<JoinCriteria> criteria;
  }
}
