package com.datasqrl.calcite.schema.sql;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlPreparingTable;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.SqrlTableFunction;
import com.datasqrl.calcite.schema.sql.SqrlToSql.Context;
import com.datasqrl.calcite.schema.sql.SqrlToSql.Result;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.plan.hints.TopNHint.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqrlCompoundIdentifier;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

@AllArgsConstructor
public class SqrlToSql implements SqlRelationVisitor<Result, Context> {
  final QueryPlanner planner;
  final AtomicInteger pkId = new AtomicInteger(0);

  public Result rewrite(SqlNode query, boolean materializeSelf, List<String> currentPath,
      Optional<SqrlTableFunctionDef> tableArgs, boolean isJoinDeclaration) {
    Context context = new Context(materializeSelf, currentPath, new HashMap<>(), false, currentPath.size() > 0, false, tableArgs, isJoinDeclaration);

    Result result = SqlNodeVisitor.accept(this, query, context);
    CalciteFixes.appendSelectLists(result.getSqlNode());
    return result;
  }

  @Override
  public Result visitQuerySpecification(SqlSelect call, Context context) {
    boolean isAggregating = hasAggs(call.getSelectList().getList());
    // Copy query specification with new RelNode.
    Context newContext = new Context(context.materializeSelf, context.currentPath, new HashMap<>(), isAggregating,
        context.isNested,call.getFetch() != null, context.tableFunctionDef, context.isJoinDeclaration);
    Result result = SqlNodeVisitor.accept(this, call.getFrom(), newContext);

    //retain distinct hint too
    if (call.isKeywordPresent(SqlSelectKeyword.DISTINCT) ||
        (context.isNested() && call.getFetch() != null)) {
      //if is nested, get primary key nodes
      int keySize = context.isNested()
          ? planner.getCatalogReader().getSqrlTable(context.currentPath).getKeys().get(0).asSet().size()
          : 0;
//      Preconditions.checkState(keySize == result.keysToPullUp.size());

      SqlSelectBuilder inner = new SqlSelectBuilder(call)
          .clearKeywords()
          .setFrom(result.getSqlNode())
          .rewriteExpressions(new WalkSubqueries(planner, newContext));
      pullUpKeys(inner, result.keysToPullUp, isAggregating);

      SqlSelectBuilder topSelect = new SqlSelectBuilder()
          .setFrom(inner.build())
          .setTopNHint(call.isKeywordPresent(SqlSelectKeyword.DISTINCT)
              ? Type.SELECT_DISTINCT : Type.TOP_N, SqlSelectBuilder.sqlIntRange(keySize))
          ;

      return new Result(topSelect.build(),
          result.getCurrentPath(), List.of(), List.of());
    }

    SqlSelectBuilder select = new SqlSelectBuilder(call)
        .setFrom(result.getSqlNode())
        .rewriteExpressions(new WalkSubqueries(planner, newContext));
    pullUpKeys(select, result.keysToPullUp, isAggregating);


    return new Result(select.build(), result.getCurrentPath(), List.of(), List.of());
  }

  private void pullUpKeys(SqlSelectBuilder inner, List<String> keysToPullUp, boolean isAggregating) {
    if (!keysToPullUp.isEmpty()) {
      inner.prependSelect(keysToPullUp);
      if (isAggregating) {
        if (inner.hasOrder()) {
          inner.prependOrder(keysToPullUp);
        }
        inner.prependGroup(keysToPullUp);
      }
    }
  }

  private boolean hasAggs(List<SqlNode> list) {
    AtomicBoolean b = new AtomicBoolean(false);
    for (SqlNode node : list) {
      node.accept(new SqlShuttle() {
        @Override
        public SqlNode visit(SqlCall call) {
          if (call.getOperator() instanceof SqlUnresolvedFunction) {
            List<SqlOperator> matches = new ArrayList<>();
            planner.getOperatorTable().lookupOperatorOverloads(call.getOperator().getNameAsId(),
                SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, matches, planner.getCatalogReader()
                    .nameMatcher());

            for (SqlOperator op : matches) {
              if (op.isAggregator()) {
                b.set(true);
              }
            }
          } else {
            if (call.getOperator().isAggregator()) {
              b.set(true);
            }
          }
          return super.visit(call);
        }
      });
    }

    return b.get();
  }

  @Override
  public Result visitAliasedRelation(SqlCall node, Context context) {
    Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);

    SqlAliasCallBuilder aliasBuilder = new SqlAliasCallBuilder(node);
    context.addAlias(aliasBuilder.getAlias(), result.getCurrentPath());

    SqlNode newNode = aliasBuilder.setTable(result.getSqlNode())
        .build();

    return new Result(newNode, result.getCurrentPath(), result.keysToPullUp, List.of());
  }

  @Override
  public Result visitTable(SqrlCompoundIdentifier node, Context context) {
    Iterator<SqlNode> input = node.getItems().iterator();
    PathWalker pathWalker = new PathWalker(planner);

    SqlNode item = input.next();

    String identifier = getIdentifier(item)
        .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));

    SqlJoinPathBuilder builder = new SqlJoinPathBuilder(planner);
    boolean isSingleTable = node.getItems().size() == 1;
    boolean isAlias = context.hasAlias(identifier);
    boolean isAggregating = context.isAggregating();
    boolean isLimit = context.isLimit();
    boolean isNested = context.isNested();
    boolean isSelf = identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical());
    boolean materializeSelf = context.isMaterializeSelf();
    boolean isSchemaTable = getIdentifier(item)
        .map(i->planner.getCatalogReader().getSqrlTable(List.of(i)) != null)
        .orElse(false);

    List<String> pullupColumns = List.of();
    if (item.getKind() == SqlKind.SELECT) {
      SqrlToSql sqrlToSql = new SqrlToSql(planner);
      Result rewrite = sqrlToSql.rewrite(item, false, context.currentPath,
          context.tableFunctionDef, false);
      RelNode relNode = planner.plan(Dialect.CALCITE, rewrite.getSqlNode());
      builder.push(rewrite.getSqlNode(), relNode.getRowType());
    } else if (isSchemaTable) {
      pathWalker.walk(identifier);
      builder.scanFunction(pathWalker.getPath(), List.of());
    } else if (isAlias) {
      if (!input.hasNext()) {
        throw new RuntimeException("Alias by itself.");
      }

      pathWalker.setPath(context.getAliasPath(identifier));
      //Walk the next one and push in table function
      String alias = identifier;
      item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));

      pathWalker.walk(nextIdentifier);

      SqlUserDefinedTableFunction fnc = SqrlRelBuilder.getSqrlTableFunction(planner, pathWalker.getPath());
      if (fnc == null) {
        builder.scanNestedTable(pathWalker.getPath());
      } else {
        List<SqlNode> args = rewriteArgs(alias, (SqrlTableFunction) fnc.getFunction());
        builder.scanFunction(fnc, args);
      }
    } else if (isSelf) {
      pathWalker.setPath(context.getCurrentPath());
      if (materializeSelf || !input.hasNext()) {//treat self as a table
        builder.scanNestedTable(context.getCurrentPath());
        if (isNested) {
          RelOptTable table = planner.getCatalogReader().getSqrlTable(pathWalker.getAbsolutePath());
          pullupColumns = IntStream.range(0, table.getKeys().get(0).asSet().size())
              .mapToObj(i -> "__" + ((SqrlPreparingTable) table).getInternalTable().getRowType().getFieldList().get(i).getName() + "$pk$" + pkId.incrementAndGet())
//              .mapToObj(i -> ((SqrlPreparingTable) table).getInternalTable().getRowType().getFieldList().get(i).getName())
              .collect(Collectors.toList());
        }
      } else { //treat self as a parameterized binding to the next function
        item = input.next();
        String nextIdentifier = getIdentifier(item)
            .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));
        pathWalker.walk(nextIdentifier);

        SqlUserDefinedTableFunction fnc = SqrlRelBuilder.getSqrlTableFunction(planner, pathWalker.getAbsolutePath());
        if (fnc == null) {
          builder.scanNestedTable(pathWalker.getPath());
        } else {
          RelDataType type = planner.getCatalogReader().getSqrlTable(context.currentPath)
              .getRowType();
          List<SqlNode> args = replaceSelfFieldsWithInputParams(
              rewriteArgs(ReservedName.SELF_IDENTIFIER.getCanonical(),
                  (SqrlTableFunction) fnc.getFunction()), type);

          builder.scanFunction(fnc, args);
        }
      }
    } else {
      throw new RuntimeException("Unknown table: " + item);
    }

    while (input.hasNext()) {
      item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      String alias = builder.getLatestAlias();
      SqlUserDefinedTableFunction fnc = SqrlRelBuilder.getSqrlTableFunction(planner, pathWalker.getPath());
      if (fnc == null) {
        builder.scanNestedTable(pathWalker.getPath());
      } else {
        List<SqlNode> args = rewriteArgs(alias,
            (SqrlTableFunction) fnc.getFunction());

        builder.scanFunction(fnc, args)
            .joinLateral();
      }
    }

    SqlNode sqlNode = builder.buildAndProjectLast(pullupColumns);

    return new Result(sqlNode, pathWalker.getAbsolutePath(), pullupColumns, List.of());
  }

  private List<SqlNode> replaceSelfFieldsWithInputParams(List<SqlNode> sqlIdentifiers,
      RelDataType type) {
    return sqlIdentifiers.stream()
        //todo: not right
        .collect(Collectors.toList());
  }

  private List<SqlNode> rewriteArgs(String alias, SqrlTableFunction function) {
    return function.getParameters().stream()
        .filter(f->f instanceof SqrlFunctionParameter)
        .map(f->(SqrlFunctionParameter)f)
        //todo: alias name not correct, look at default value and extract dynamic param with correct field name
        // but this is okay for now
        .map(f->new SqlIdentifier(List.of(alias, f.getName().split("@")[1]), SqlParserPos.ZERO))
        .collect(Collectors.toList());
  }

  private Optional<String> getIdentifier(SqlNode item) {
    if (item instanceof SqlIdentifier) {
      return Optional.of(((SqlIdentifier) item).getSimple());
    } else if (item instanceof SqlCall) {
      return Optional.of(((SqlCall) item).getOperator().getName());
    }

    return Optional.empty();
  }

  @Override
  public Result visitJoin(SqlJoin call, Context context) {
    Result leftNode = SqlNodeVisitor.accept(this, call.getLeft(), context);

    Context context1 = new Context(false, leftNode.currentPath,  context.aliasPathMap, false,
        false, false,context.tableFunctionDef, false);
    Result rightNode = SqlNodeVisitor.accept(this, call.getRight(), context1);

    SqlNode join = new SqlJoinBuilder(call)
        .rewriteExpressions(new WalkSubqueries(planner, context))
        .setLeft(leftNode.getSqlNode())
        .setRight(rightNode.getSqlNode())
        .lateral()
        .build();

    return new Result(join, rightNode.getCurrentPath(), leftNode.keysToPullUp, List.of());
  }

  @Override
  public Result visitSetOperation(SqlCall node, Context context) {
    return new Result(
        node.getOperator().createCall(node.getParserPosition(),
            node.getOperandList().stream()
            .map(o->SqlNodeVisitor.accept(this, o, context).getSqlNode())
            .collect(Collectors.toList())),
        List.of(),
        List.of(),
        List.of());
  }

  @AllArgsConstructor
  public static class WalkSubqueries extends SqlShuttle {
    QueryPlanner planner;
    Context context;
    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        SqrlToSql sqrlToSql = new SqrlToSql(planner);
        Result result = sqrlToSql.rewrite(call, false, context.currentPath, context.tableFunctionDef,
            false);

        return result.getSqlNode();
      }

      return super.visit(call);
    }
  }

  @Value
  public static class Result {
    SqlNode sqlNode;
    List<String> currentPath;
    List<String> keysToPullUp;
    List<List<String>> tableReferences;
  }

  @Value
  public static class Context {
    //unbound replaces @ with system args, bound expands @ to table.
    boolean materializeSelf;
    List<String> currentPath;
    Map<String, List<String>> aliasPathMap;
    public boolean isAggregating;
    public boolean isNested;
    public boolean isLimit;

    Optional<SqrlTableFunctionDef> tableFunctionDef;
    public boolean isJoinDeclaration;

    public void addAlias(String alias, List<String> currentPath) {
      aliasPathMap.put(alias, currentPath);
    }

    public boolean hasAlias(String alias) {
      return aliasPathMap.containsKey(alias);
    }

    public List<String> getAliasPath(String alias) {
      return new ArrayList<>(getAliasPathMap().get(alias));
    }
  }
}
