package com.datasqrl.plan.validate;

import static com.datasqrl.plan.validate.ScriptValidator.addError;
import static org.apache.calcite.sql.SqlUtil.stripAs;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlAliasCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.plan.validate.SqrlToValidatorSql.Context;
import com.datasqrl.plan.validate.SqrlToValidatorSql.Result;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

@AllArgsConstructor
@Getter
public class SqrlToValidatorSql implements SqlRelationVisitor<Result, Context> {

  final QueryPlanner planner;
  final ErrorCollector errorCollector;
  final AtomicInteger uniqueId;
  private final Multimap<SqlNode, TableFunction> isA = ArrayListMultimap.create();
  private final List<SqlFunction> plannerFns = new ArrayList<>();
  final SqlNameUtil nameUtil;

  public Result rewrite(SqlNode query, NamePath currentPath, SqrlTableFunctionDef tableArgs) {
    Context context = new Context(currentPath, new HashMap<>(), tableArgs, query);

    return SqlNodeVisitor.accept(this, query, context);
  }

  @Override
  public Result visitQuerySpecification(SqlSelect call, Context context) {
    // Copy query specification with new RelNode.
    Context newContext = new Context(context.currentPath, new HashMap<>(), context.tableFunctionDef,
        context.root);
    Result result = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getFrom()), newContext);

    for (SqlNode node : call.getSelectList()) {
      node = stripAs(node);
      if (node instanceof SqlIdentifier) {
        SqlIdentifier ident = (SqlIdentifier) node;
        if (ident.isStar() && ident.names.size() == 1) {
          for (NamePath path : newContext.getAliasPathMap().values()) {
            Collection<Function> sqrlTable = planner.getSchema().getFunctions(path.getDisplay(), false);

            if (!sqrlTable.isEmpty()) {
              new ArrayList<>(sqrlTable).stream()
                      .map(f->(SqrlTableMacro)f)
                          .forEach(f->isA.put(context.root, f));
            }
          }
        } else if (ident.isStar() && ident.names.size() == 2) {
          NamePath path = newContext.getAliasPath(nameUtil.toName(ident.names.get(0)));
          Collection<Function> sqrlTable = planner.getSchema().getFunctions(path.getDisplay(), false);

          if (!sqrlTable.isEmpty()) {
            new ArrayList<>(sqrlTable).stream()
                .map(f->(SqrlTableMacro)f)
                .forEach(f->isA.put(context.root, f));
          }
        }
      }
    }

    // Todo: check distinct rules

    SqlSelectBuilder select = new SqlSelectBuilder(call)
        .setFrom(result.getSqlNode())
        .rewriteExpressions(new WalkSubqueries(planner, newContext));

    return new Result(select.build(), result.getCurrentPath(), result.getFncs());
  }

  private SqlNode appendAliasIfRequired(SqlNode sqlNode) {
    if (sqlNode instanceof SqlIdentifier) {
      if (((SqlIdentifier) sqlNode).names.size() == 1) {
        return SqlStdOperatorTable.AS.createCall(sqlNode.getParserPosition(),
            sqlNode,
            sqlNode);
      }
    }

    if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier) sqlNode).names.size() == 1) {
      return SqlStdOperatorTable.AS.createCall(sqlNode.getParserPosition(),
          sqlNode, sqlNode);
    }

    return sqlNode;
  }

  @Override
  public Result visitAliasedRelation(SqlCall node, Context context) {
    Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
    SqlAliasCallBuilder aliasBuilder = new SqlAliasCallBuilder(node);

    context.addAlias(nameUtil.toName(aliasBuilder.getAlias()), result.getCurrentPath());

    SqlNode newNode = aliasBuilder.setTable(result.getSqlNode())
        .build();

    return new Result(newNode, result.getCurrentPath(), result.getFncs());
  }

  @Override
  public Result visitTable(SqlIdentifier node, Context context) {
    List<SqlNode> items = new ArrayList<>();
    for (int i = 0; i < node.names.size(); i++) {
      items.add(new SqlIdentifier(node.names.get(i), node.getComponentParserPosition(i)));
    }

    Iterator<SqlNode> input = items.iterator();

    PathWalker pathWalker = new PathWalker(planner.getCatalogReader());

    SqlNode item = input.next();

    if (item.getKind() == SqlKind.SELECT) {
      Context ctx = new Context(context.currentPath, new HashMap<>(), context.tableFunctionDef,
          context.root);

      return SqlNodeVisitor.accept(this, item, ctx);
    }

    Name identifier = getIdentifier(item)
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

    boolean isAlias = context.hasAlias(identifier);
    boolean isSelf = identifier.equals(ReservedName.SELF_IDENTIFIER);
    Collection<Function> tableFunction = planner.getSchema().getFunctions(
        identifier.getDisplay(), false);

    if (!tableFunction.isEmpty()) {
      pathWalker.walk(identifier);
    } else if (isAlias) {
      if (!input.hasNext()) {
        throw addError(errorCollector, ErrorLabel.GENERIC, item, "Alias by itself.");
      }
      pathWalker.setPath(context.getAliasPath(identifier));
      //Walk the next one and push in table function
      item = input.next();

      Optional<Name> nextIdentifier = getIdentifier(item);
      if (nextIdentifier.isEmpty()) {
        throw addError(errorCollector, ErrorLabel.GENERIC, item,
            "Table is not a valid identifier");
      }

      pathWalker.walk(nextIdentifier.get());
      //get table of current path (no args)
      Collection<Function> table = planner.getSchema().getFunctions(
          pathWalker.getPath().getDisplay(), false);

      if (table.isEmpty()) {
        throw addError(errorCollector, ErrorLabel.GENERIC, item, "Could not find path: %s",
            pathWalker.getUserDefined().getDisplay());
      }
    } else if (isSelf) {
      pathWalker.setPath(context.getCurrentPath());
      if (!input.hasNext()) {//treat self as a table
        Collection<Function> table = planner.getSchema().getFunctions(
            context.getCurrentPath().getDisplay(), false);
        if (table.isEmpty()) {
          throw addError(errorCollector, ErrorLabel.GENERIC, item, "Could not find parent table: %s",
              context.getCurrentPath().getDisplay());
        }
      } else { //treat self as a parameterized binding to the next function
        item = input.next();
        Optional<Name> nextIdentifier = getIdentifier(item);
        if (nextIdentifier.isEmpty()) {
          throw addError(errorCollector, ErrorLabel.GENERIC, item, "Table is not a valid identifier");
        }
        pathWalker.walk(nextIdentifier.get());

        Collection<Function> table =
            planner.getSchema().getFunctions(pathWalker.getPath().getDisplay(), false);
        if (table.isEmpty()) {
          throw addError(errorCollector, ErrorLabel.GENERIC, item, "Could not find table: %s",
              pathWalker.getUserDefined().getDisplay());
        }
      }
    } else {
      throw addError(errorCollector, ErrorLabel.GENERIC, item, "Could not find table: %s",
          identifier);
    }

    while (input.hasNext()) {
      item = input.next();
      Optional<Name> nextIdentifier = getIdentifier(item);
      if (nextIdentifier.isEmpty()) {
        throw addError(errorCollector, ErrorLabel.GENERIC, item, "Table is not a valid identifier");
      }
      pathWalker.walk(nextIdentifier.get());

      Collection<Function> table =
          planner.getSchema().getFunctions(pathWalker.getPath().getDisplay(), false);
      if (table.isEmpty()) {
        throw addError(errorCollector, ErrorLabel.GENERIC, item, "Could not find table: %s",
            pathWalker.getUserDefined().getDisplay());
      }
    }

    SqlCall call1 = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO,
        new SqlIdentifier(pathWalker.getAbsolutePath().getDisplay(), SqlParserPos.ZERO));
    return new Result(call1, pathWalker.getAbsolutePath(), plannerFns);
  }

  @Override
  public Result visitCall(SqlCall node, Context context) {
    throw addError(errorCollector, ErrorLabel.GENERIC, node, "Call not yet supported %s",
        node.getOperator().getName());
  }

  private Optional<Name> getIdentifier(SqlNode item) {
    if (item instanceof SqlIdentifier) {
      return Optional.of(((SqlIdentifier) item).getSimple())
          .map(nameUtil::toName);
    } else if (item instanceof SqlCall) {
      return Optional.of(((SqlCall) item).getOperator().getName())
          .map(nameUtil::toName);
    }

    return Optional.empty();
  }

  @Override
  public Result visitJoin(
      SqlJoin call, Context context) {
    Result leftNode = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getLeft()), context);

    Context context1 = new Context(leftNode.currentPath, context.aliasPathMap,
        context.tableFunctionDef, context.root);
    Result rightNode = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getRight()), context1);

    SqlNode join = new SqlJoinBuilder(call)
        .rewriteExpressions(new WalkSubqueries(planner, context))
        .setLeft(leftNode.getSqlNode())
        .setRight(rightNode.getSqlNode())
        .lateral()
        .build();

    return new Result(join, rightNode.getCurrentPath(), plannerFns);
  }

  @Override
  public Result visitSetOperation(SqlCall node, Context context) {
    return new Result(
        node.getOperator().createCall(node.getParserPosition(),
            node.getOperandList().stream()
                .map(o -> SqlNodeVisitor.accept(this, o, context).getSqlNode())
                .collect(Collectors.toList())),
        NamePath.ROOT, plannerFns);
  }

  @Override
  public Result visitCollectTableFunction(SqlCall node, Context context) {
    return visitAugmentedTable(node, context);
  }

  @Override
  public Result visitLateralFunction(SqlCall node, Context context) {
    return visitAugmentedTable(node, context);
  }

  @Override
  public Result visitUnnestFunction(SqlCall node, Context context) {
    return visitAugmentedTable(node, context);
  }

  private Result visitAugmentedTable(SqlCall node, Context context) {
    Preconditions.checkState(node.getOperandList().size() == 1, "Expected a single table condition (LATERAL, UNNEST, ...)");
    Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
    SqlCall call = node.getOperator().createCall(node.getParserPosition(), result.sqlNode);
    //We don't actually fully resolve the function, just check that it exists and let the sql validator do the rest
    return new Result(call, result.currentPath, result.fncs);
  }

  @Override
  public Result visitUserDefinedTableFunction(SqlCall node, Context context) {
    List<SqlOperator> operators = new ArrayList<>();
    planner.getOperatorTable().lookupOperatorOverloads(node.getOperator().getNameAsId(),
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION, operators,
        planner.getCatalogReader().nameMatcher());

    if (operators.isEmpty()) {
      throw addError(errorCollector, ErrorLabel.GENERIC, node, "Could not find table function %s",
          node.getOperator().getName());
    }
    return new Result(node, NamePath.ROOT, List.of());
  }

  @AllArgsConstructor
  public class WalkSubqueries extends SqlShuttle {

    QueryPlanner planner;
    Context context;

    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        Result result = rewrite(call, context.currentPath, context.tableFunctionDef);
        return result.getSqlNode();
      }

      return super.visit(call);
    }
  }


  @Value
  public class Result {
    SqlNode sqlNode;
    NamePath currentPath;
    List<SqlFunction> fncs;
  }

  @Value
  public class Context {
    //unbound replaces @ with system args, bound expands @ to table.
    NamePath currentPath;
    Map<Name, NamePath> aliasPathMap;
    SqrlTableFunctionDef tableFunctionDef;
    SqlNode root;

    public void addAlias(Name alias, NamePath currentPath) {
      aliasPathMap.put(alias, currentPath);
    }

    public boolean hasAlias(Name alias) {
      return aliasPathMap.containsKey(alias);
    }

    public NamePath getAliasPath(Name alias) {
      if (getAliasPathMap().get(alias) == null) {
        throw new RuntimeException("Could not find alias: " + alias);
      }
      return getAliasPathMap().get(alias);
    }
  }
}