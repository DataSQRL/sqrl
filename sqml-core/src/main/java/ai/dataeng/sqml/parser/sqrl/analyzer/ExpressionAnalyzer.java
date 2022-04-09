package ai.dataeng.sqml.parser.sqrl.analyzer;


import static ai.dataeng.sqml.parser.sqrl.AliasUtil.aliasMany;
import static ai.dataeng.sqml.parser.sqrl.AliasUtil.selectAliasItem;
import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.getCriteria;
import static ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.expandRelation;
import static ai.dataeng.sqml.util.SqrlNodeUtil.alias;
import static ai.dataeng.sqml.util.SqrlNodeUtil.function;
import static ai.dataeng.sqml.util.SqrlNodeUtil.group;
import static ai.dataeng.sqml.util.SqrlNodeUtil.ident;
import static ai.dataeng.sqml.util.SqrlNodeUtil.query;
import static ai.dataeng.sqml.util.SqrlNodeUtil.select;
import static ai.dataeng.sqml.util.SqrlNodeUtil.selectAlias;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.sqrl.PathUtil;
import ai.dataeng.sqml.parser.sqrl.analyzer.Scope.ResolveResult;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.function.RewritingFunction;
import ai.dataeng.sqml.parser.sqrl.function.SqlNativeFunction;
import ai.dataeng.sqml.parser.sqrl.function.SqrlFunction;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.commons.collections.ListUtils;

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
          /*
           * Replace current token with an Identifier and expand the path into a subquery. This
           * subquery is joined to the table it was resolved from.
           */
          Name baseTableAlias = gen.nextTableAliasName();
          Relation relation = new TableNode(Optional.empty(), result.getTable().getId().toNamePath(), Optional.of(baseTableAlias));
          TableBookkeeping b = new TableBookkeeping(relation, baseTableAlias, result.getTable());
          NamePath remaining = result.getRemaining().get();
          Field lastField = null;
          for (int i = 0; i < remaining.getLength(); i++) {
            lastField = b.getCurrentTable().getField(remaining.get(i));
            if (!(lastField instanceof Relationship)) break;
            Relationship rel = (Relationship)lastField;
            Name alias = gen.nextTableAliasName();
            Join join = new Join(Optional.empty(), Type.INNER, b.getCurrent(),
                expandRelation(rel, alias), getCriteria(rel, b.getAlias(), alias));
            b = new TableBookkeeping(join, alias, rel.getToTable());
          }

          Name tableAlias = gen.nextTableAliasName();
          Name fieldAlias = gen.nextAliasName();
          Query query = query(
              select(ListUtils.union(
                  selectAliasItem(result.getTable().getPrimaryKeys(), baseTableAlias),
                  List.of(selectAlias(
                      function(node.getName(), alias(b.getAlias(),
                        lastField instanceof Relationship ? ((Relationship) lastField).getToTable().getPrimaryKeys().get(0).getId() : lastField.getId())),
                      fieldAlias.toNamePath()
                  )))),
              b.getCurrent(),
              group(aliasMany(result.getTable().getPrimaryKeys(), baseTableAlias))
          );
          AliasedRelation subquery = new AliasedRelation(new TableSubquery(query), ident(tableAlias));

          joinResults.add(new JoinResult(Type.LEFT, subquery, getCriteria(result.getTable().getPrimaryKeys(), result.getAlias(), tableAlias)));
          return new Identifier(Optional.empty(), NamePath.of(tableAlias, fieldAlias));
        }
      }

      return new FunctionCall(node.getLocation(), node.getName(), arguments,
          node.isDistinct(), node.getOver());
    }

    @Override
    public Expression rewriteIdentifier(Identifier node, Context context,
        ExpressionTreeRewriter<Context> treeRewriter) {
//      List<FieldPath> resolved = context.getScope()
//          .resolveField(node.getNamePath());
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
        Relation relation = expandRelation((Relationship)result.getFirstField(), baseTableAlias);

        joinResults.add(new JoinResult(
            Type.LEFT, relation,
            getCriteria((Relationship) result.getFirstField(), result.getAlias(), baseTableAlias)
        ));
        return new Identifier(node.getLocation(), baseTableAlias.toNamePath().concat(
            result.getRemaining().get().getLast()));
      }

      NamePath qualifiedName = context.getScope().qualify(node.getNamePath());
      return new Identifier(node.getLocation(), qualifiedName);
    }
  }

  @Value
  public static class JoinResult {
    Type type;
    Relation relation;
    Optional<JoinCriteria> criteria;
  }
}
