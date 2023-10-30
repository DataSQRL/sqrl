package com.datasqrl.calcite;

import static com.datasqrl.plan.validate.ScriptValidator.isSelfTable;
import static com.datasqrl.plan.validate.ScriptValidator.isVariable;

import com.datasqrl.calcite.SqrlToSql.Context;
import com.datasqrl.calcite.SqrlToSql.Result;
import com.datasqrl.calcite.NormalizeTablePath.PathItem;
import com.datasqrl.calcite.NormalizeTablePath.SelfTablePathItem;
import com.datasqrl.calcite.NormalizeTablePath.TablePathResult;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlAliasCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.sqrl.PathToSql;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.plan.hints.TopNHint.Type;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class SqrlToSql implements SqlRelationVisitor<Result, Context> {
  private final CatalogReader catalogReader;
  private final SqlOperatorTable operatorTable;
  private final NormalizeTablePath normalizeTablePath;
  private List<FunctionParameter> parameters;
  private final AtomicInteger uniquePkId;
  private final SqlNameUtil nameUtil;

  public SqrlToSql(CatalogReader catalogReader, SqlOperatorTable operatorTable,
      NormalizeTablePath normalizeTablePath, List<FunctionParameter> parameters,
      AtomicInteger uniquePkId, SqlNameUtil nameUtil) {
    this.catalogReader = catalogReader;
    this.operatorTable = operatorTable;
    this.normalizeTablePath = normalizeTablePath;
    this.parameters = parameters;
    this.uniquePkId = uniquePkId;
    this.nameUtil = nameUtil;
  }

  public Result rewrite(SqlNode query, boolean materializeSelf, NamePath currentPath) {
    Context context = new Context.ContextBuilder(materializeSelf, currentPath, currentPath.size() > 0).build();
    Result result = SqlNodeVisitor.accept(this, query, context);
    CalciteFixes.appendSelectLists(result.getSqlNode());
    return result;
  }

  @Override
  public Result visitQuerySpecification(SqlSelect call, Context context) {
    boolean isAggregating = hasAggs(call.getSelectList().getList());
    // Update context for current select details
    Context newContext = new Context.ContextBuilder(context)
        .setAliasPathMap(new HashMap<>()) //clear any alias mapping
        .build();
    Result result = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getFrom()), newContext);

    //retain distinct hint too
    if (isDistinctOnHintPresent(call)) {
      List<Integer> hintOps = IntStream.range(0, call.getSelectList().size()).boxed()
          .collect(Collectors.toList());
      SqlSelectBuilder sqlSelectBuilder = new SqlSelectBuilder(call)
          .setLimit(1)
          .clearKeywords()
          .setFrom(result.sqlNode);

      List<SqlNode> selectList = new ArrayList<>(call.getSelectList().getList());
      Set<String> fieldNames = new HashSet<>(getFieldNames(selectList));

      List<String> columns = catalogReader.getTableFromPath(result.getCurrentPath())
          .unwrap(ModifiableTable.class).getRowType().getFieldNames();
      List<SqlNode> newNodes = new ArrayList<>();
      for (String column : columns) {
        if (!fieldNames.contains(column)) {
          newNodes.add(new SqlIdentifier(column, SqlParserPos.ZERO));
          fieldNames.add(column);
        }
      }

      selectList.addAll(newNodes);
      sqlSelectBuilder.setSelectList(selectList).clearHints();
      result.condition.ifPresent(sqlSelectBuilder::appendWhere);
      SqlSelect top = new SqlSelectBuilder().setFrom(sqlSelectBuilder.build()).setDistinctOnHint(hintOps).build();
      return new Result(top, result.getCurrentPath(), List.of(), List.of(), Optional.empty(), result.params);
    } else if (call.isKeywordPresent(SqlSelectKeyword.DISTINCT) || (context.isNested() && call.getFetch() != null)) {
      //if is nested, get primary key nodes
      int keySize = context.isNested()
          ? catalogReader.getTableFromPath(context.currentPath).getKeys().get(0)
          .asSet().size()
          : 0;

      SqlSelectBuilder inner = new SqlSelectBuilder(call)
          .clearKeywords()
          .setFrom(result.getSqlNode())
          .rewriteExpressions(new WalkExpressions(newContext));
      pullUpKeys(inner, result.pullupColumns, isAggregating);
      result.condition
          .ifPresent(inner::appendWhere);
      SqlSelectBuilder topSelect = new SqlSelectBuilder()
          .setFrom(inner.build())
          .setTopNHint(call.isKeywordPresent(SqlSelectKeyword.DISTINCT)
              ? Type.SELECT_DISTINCT : Type.TOP_N, SqlSelectBuilder.sqlIntRange(keySize));

      return new Result(topSelect.build(),
          result.getCurrentPath(), List.of(), List.of(),
          Optional.empty(), result.params);
    }

    SqlSelectBuilder select = new SqlSelectBuilder(call)
        .setFrom(result.getSqlNode())
        .rewriteExpressions(new WalkExpressions(newContext));

    result.condition
        .ifPresent(select::appendWhere);

    pullUpKeys(select, result.pullupColumns, isAggregating);

    return new Result(select.build(), result.getCurrentPath(), List.of(), List.of(), Optional.empty(),
        result.params);
  }

  private SqlNode appendAliasIfRequired(SqlNode sqlNode) {
    if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier) sqlNode).names.size() == 1) {
      return SqlStdOperatorTable.AS.createCall(sqlNode.getParserPosition(), sqlNode, sqlNode);
    }
    return sqlNode;
  }

  private List<String> getFieldNames(List<SqlNode> list) {
    List<String> nodes = new ArrayList<>();
    for (SqlNode node : list) {
      String name;
      if (node instanceof SqlIdentifier) {
        name = ((SqlIdentifier) node).names.get(((SqlIdentifier) node).names.size() - 1);
      } else if (node instanceof SqlCall && node.getKind() == SqlKind.AS) {
        name = ((SqlIdentifier) ((SqlCall) node).getOperandList().get(1)).names.get(0);
      } else {
        throw new RuntimeException("Could not derive name: " + node);
      }
      nodes.add(name);
    }

    return nodes;
  }

  private boolean isDistinctOnHintPresent(SqlSelect call) {
    return call.getHints().getList().stream()
        .anyMatch(f -> ((SqlHint) f).getName().equalsIgnoreCase("DISTINCT_ON"));
  }

  private void pullUpKeys(SqlSelectBuilder inner, List<PullupColumn> keysToPullUp, boolean isAggregating) {
    if (keysToPullUp.isEmpty()) {
      return;
    }

    inner.prependSelect(keysToPullUp);

    if (isAggregating) {
      if (inner.hasOrder()) {
        inner.prependOrder(keysToPullUp);
      }
      inner.prependGroup(keysToPullUp);
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
            operatorTable.lookupOperatorOverloads(call.getOperator().getNameAsId(),
                SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, matches,
                catalogReader.nameMatcher());

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
    context.addAlias(nameUtil.toName(aliasBuilder.getAlias()), result.getCurrentPath());

    SqlNode newNode = aliasBuilder.setTable(result.getSqlNode())
        .build();

    return new Result(newNode, result.getCurrentPath(), result.pullupColumns, List.of(), result.getCondition(),
        result.params);
  }

  @Override
  public Result visitTable(SqlIdentifier node, Context context) {
    List<SqlNode> items = node.names.stream()
        .map(name -> new SqlIdentifier(name, node.getComponentParserPosition(node.names.indexOf(name))))
        .collect(Collectors.toList());

    TablePathResult result = normalizeTablePath.convert(items, context, parameters);

    PathToSql pathToSql = new PathToSql();
    SqlNode sqlNode = pathToSql.build(result.getPathItems());

    List<PullupColumn> pullupColumns = (context.isNested && result.getPathItems().get(0) instanceof SelfTablePathItem)
        ? buildPullupColumns((SelfTablePathItem)result.getPathItems().get(0))
        : List.of();
    // Wrap in a select to maintain sql semantics
    if (!pullupColumns.isEmpty() || result.getPathItems().size() > 1) {
      sqlNode = buildAndProjectLast(pullupColumns, sqlNode, result.getPathItems().get(0),
          result.getPathItems().get(result.getPathItems().size()-1));
    } else if (result.getPathItems().size() == 1) {
      //Just a table by itself
      if (sqlNode instanceof SqlBasicCall) {
        sqlNode = ((SqlBasicCall) sqlNode).getOperandList().get(0);
      }
    }

    this.parameters = result.getParams(); //update parameters with new parameters
    return new Result(sqlNode, result.getPath(), pullupColumns,
        List.of(), Optional.empty(), result.getParams());
  }


  public SqlNode buildAndProjectLast(List<PullupColumn> pullupCols, SqlNode sqlNode,
      PathItem first, PathItem last) {

    SqlSelectBuilder select = new SqlSelectBuilder()
        .setFrom(sqlNode);

    List<SqlNode> selectList = new ArrayList<>();
    for (PullupColumn column : pullupCols) {
      selectList.add(
          SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
              new SqlIdentifier(List.of(first.getAlias().getDisplay(), column.getColumnName()), SqlParserPos.ZERO),
              new SqlIdentifier(column.getDisplayName(), SqlParserPos.ZERO)));
    }

    selectList.add(new SqlIdentifier(List.of(last.getAlias().getDisplay(), ""), SqlParserPos.ZERO));

    select.setSelectList(selectList);
    return select.build();
  }

  private List<PullupColumn> buildPullupColumns(SelfTablePathItem selfTablePathItem) {
    RelOptTable table = selfTablePathItem.table;
    return IntStream.range(0, table.getKeys().get(0).asSet().size())
        .mapToObj(i -> new PullupColumn(
            table.getRowType().getFieldList().get(i).getName(),
            String.format("%spk%d$%s", ReservedName.SYSTEM_HIDDEN_PREFIX, uniquePkId.incrementAndGet(),
                table.getRowType().getFieldList().get(i).getName()),
            String.format("%spk%d$%s", ReservedName.SYSTEM_HIDDEN_PREFIX, i + 1,
                table.getRowType().getFieldList().get(i).getName())
        ))
        .collect(Collectors.toList());
  }

  @Override
  public Result visitJoin(SqlJoin call, Context context) {
    //Check if we should skip the lhs, if it's self and we don't materialize and there is no condition
    if (isSelfTable(call.getLeft())
        && !context.isMaterializeSelf()) {
      Optional<SqlNode> condition = Optional.ofNullable(call.getCondition());
      Result result = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getRight()), context);
      return new Result(result.sqlNode, result.currentPath, result.pullupColumns, result.tableReferences, condition,
          result.params);
    }

    Result leftNode = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getLeft()), context);

    Context rhsContext = new Context.ContextBuilder(context)
        .setCurrentPath(leftNode.currentPath)
        .setNested(false)
        .build();
    Result rightNode = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getRight()), rhsContext);

    SqlNode join = new SqlJoinBuilder(call)
        .rewriteExpressions(new WalkExpressions(context))
        .setLeft(leftNode.getSqlNode())
        .setRight(rightNode.getSqlNode())
        .lateral()
        .build();

    return new Result(join, rightNode.getCurrentPath(), leftNode.pullupColumns, List.of(),
        leftNode.getCondition().or(rightNode::getCondition), leftNode.params);
  }

  @Override
  public Result visitSetOperation(SqlCall node, Context context) {
    final List<Result> operandResults = node.getOperandList().stream()
        .map(o -> SqlNodeVisitor.accept(this, o, context))
        .collect(Collectors.toList());

    Optional<SqlNode> condition = operandResults.stream()
        .map(r -> r.condition)
        .flatMap(Optional::stream)
        .findAny();
    if (condition.isPresent()) {
      throw new RuntimeException("Trailing condition abandoned in rewriting");
    }

    Optional<PullupColumn> pullupColumn = operandResults.stream()
        .map(Result::getPullupColumns)
        .flatMap(Collection::stream)
        .findAny();
    if (pullupColumn.isPresent()) {
      throw new RuntimeException("Primary key columns not pulled up in rewriting");
    }

    List<NamePath> tableReferences = operandResults.stream()
        .map(o -> o.tableReferences)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    List<SqlNode> operands = operandResults.stream()
        .map(Result::getSqlNode)
        .collect(Collectors.toList());

    SqlCall call = new SqlCallBuilder(node)
        .setOperands(operands)
        .build();

    return new Result(call,
        NamePath.ROOT,
        List.of(),
        tableReferences,
        Optional.empty(),
        operandResults.get(0).params);
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

  public Result visitAugmentedTable(SqlCall node, Context context) {
    Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
    SqlCall call = node.getOperator().createCall(node.getParserPosition(), result.sqlNode);
    return new Result(call, result.currentPath, result.pullupColumns, result.tableReferences,
        result.condition, result.params);
  }

  @Override
  public Result visitUserDefinedTableFunction(SqlCall node, Context context) {
    return new Result(node, NamePath.ROOT, List.of(), List.of(), Optional.empty(), parameters);
  }

  @Override
  public Result visitCall(SqlCall node, Context context) {
    throw new RuntimeException("Expected call");
  }

  @Value
  public static class PullupColumn {
    String columnName;
    String displayName;
    String finalName;
  }

  @AllArgsConstructor
  public class WalkExpressions extends SqlShuttle {

    Context context;

    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        Result result = rewrite(call, false, context.currentPath);

        return result.getSqlNode();
      }

      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      Preconditions.checkState(!isVariable(nameUtil.toNamePath(id.names)), "Found variable when expecting one.");
      return super.visit(id);
    }
  }

  @Value
  public static class Result {
    SqlNode sqlNode;
    NamePath currentPath;
    List<PullupColumn> pullupColumns;
    List<NamePath> tableReferences;
    Optional<SqlNode> condition;
    List<FunctionParameter> params;
  }

  @Value
  public static class Context {

    //unbound replaces @ with system args, bound expands @ to table.
    boolean materializeSelf;
    NamePath currentPath;
    Map<Name, NamePath> aliasPathMap;
    public boolean isNested;

    public void addAlias(Name alias, NamePath currentPath) {
      aliasPathMap.put(alias, currentPath);
    }

    public boolean hasAlias(Name alias) {
      return aliasPathMap.containsKey(alias);
    }

    public NamePath getAliasPath(Name alias) {
      return getAliasPathMap().get(alias);
    }

    public static class ContextBuilder {
      boolean materializeSelf;
      NamePath currentPath;
      Map<Name, NamePath> aliasPathMap;
      public boolean isNested;

      public ContextBuilder(Context context) {
        this.materializeSelf = context.materializeSelf;
        this.currentPath = context.currentPath;
        this.aliasPathMap = context.aliasPathMap;
        this.isNested = context.isNested;
      }

      public ContextBuilder(boolean materializeSelf, NamePath currentPath,
          boolean isNested) {
        this.materializeSelf = materializeSelf;
        this.currentPath = currentPath;
        this.aliasPathMap = new HashMap<>();
        this.isNested = isNested;
      }

      public ContextBuilder setMaterializeSelf(boolean materializeSelf) {
        this.materializeSelf = materializeSelf;
        return this;
      }

      public ContextBuilder setCurrentPath(NamePath currentPath) {
        this.currentPath = currentPath;
        return this;
      }

      public ContextBuilder setAliasPathMap(
          Map<Name, NamePath> aliasPathMap) {
        this.aliasPathMap = aliasPathMap;
        return this;
      }

      public ContextBuilder setNested(boolean nested) {
        this.isNested = nested;
        return this;
      }

      public Context build() {
        return new Context(materializeSelf, currentPath, aliasPathMap, isNested);
      }
    }
  }
}