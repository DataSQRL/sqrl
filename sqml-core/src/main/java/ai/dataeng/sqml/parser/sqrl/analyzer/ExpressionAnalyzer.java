package ai.dataeng.sqml.parser.sqrl.analyzer;


import static ai.dataeng.sqml.parser.sqrl.AliasUtil.selectAliasItem;
import static ai.dataeng.sqml.parser.sqrl.JoinWalker.createTableCriteria;
import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.getCriteria;
import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.expandRelation;
import static ai.dataeng.sqml.util.SqrlNodeUtil.alias;
import static ai.dataeng.sqml.util.SqrlNodeUtil.and;
import static ai.dataeng.sqml.util.SqrlNodeUtil.function;
import static ai.dataeng.sqml.util.SqrlNodeUtil.ident;
import static ai.dataeng.sqml.util.SqrlNodeUtil.selectAlias;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.TableFactory;
import ai.dataeng.sqml.parser.sqrl.JoinWalker;
import ai.dataeng.sqml.parser.sqrl.JoinWalker.TableItem;
import ai.dataeng.sqml.parser.sqrl.JoinWalker.WalkResult;
import ai.dataeng.sqml.parser.sqrl.PathUtil;
import ai.dataeng.sqml.parser.sqrl.analyzer.Scope.ResolveResult;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.function.RewritingFunction;
import ai.dataeng.sqml.parser.sqrl.function.SqlNativeFunction;
import ai.dataeng.sqml.parser.sqrl.function.SqrlFunction;
import ai.dataeng.sqml.parser.sqrl.transformers.Transformers;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.Window;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
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

      //Todo: allow expanding aggregates more than a single argument
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
              result.getRemaining().get(), //The fields from the base that we should resolve
              Optional.empty(), //push in relation
              context.getScope().getJoinScope()
          );
          TableItem first = walkResult.getTableStack().get(0);
          TableItem last = walkResult.getTableStack().get(walkResult.getTableStack().size() - 1);

          //Special case: count(entries) => count(e._uuid) FROM entries
          Field field = result.getTable().walkField(result.getRemaining().get()).get();
          Name fieldName = field instanceof Relationship ?
              ((Relationship) field).getToTable().getPrimaryKeys().get(0).getId()
              : result.getRemaining().get().getLast();

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

          TableSubquery tableSubquery = new TableSubquery(
              new Query(Optional.empty(), contextSubquery, Optional.empty(), Optional.empty()));

          TableFactory tableFactory = new TableFactory();
          Table table = tableFactory.create(tableSubquery);

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
      Table table = scope.getJoinScope(Name.SELF_IDENTIFIER).get();
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

      if (results.size() != 1) {
        System.out.println();
      }
      Preconditions.checkState(results.size() == 1,
          "Could not resolve field (ambiguous or non-existent: " + node + " : " + results + ")");

      if (PathUtil.isToOne(results.get(0))) {
        ResolveResult result = results.get(0);
        /*
         * Replace current token with an Identifier and expand the path into a subquery. This
         * subquery is joined to the table it was resolved from.
         */
        Name baseTableAlias = gen.nextTableAliasName();
        Relation relation = expandRelation((Relationship)result.getFirstField(), baseTableAlias);

        joinResults.add(new JoinResult(
            Type.LEFT, relation,
            getCriteria((Relationship) result.getFirstField(), result.getAlias(), baseTableAlias,
                Optional.empty())
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
