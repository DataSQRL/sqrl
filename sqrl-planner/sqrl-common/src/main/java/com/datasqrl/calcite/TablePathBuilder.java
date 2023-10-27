package com.datasqrl.calcite;

import static com.datasqrl.plan.validate.ScriptValidator.isSelfField;

import com.datasqrl.calcite.SqrlToSql.Context;
import com.datasqrl.calcite.SqrlToSql.PullupColumn;
import com.datasqrl.calcite.SqrlToSql.Result;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.sql.SqlJoinPathBuilder;
import com.datasqrl.calcite.sqrl.CatalogResolver;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.util.CalciteUtil.RelDataTypeFieldBuilder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.commons.collections.ListUtils;

public class TablePathBuilder {
  private static final String ALIAS_PREFIX = "_t";
  private final CatalogResolver catalogResolver;
  private final Map<FunctionParameter, SqlDynamicParam> paramMapping;
  private final AtomicInteger uniquePkId;

  @Getter
  final List<Frame> tableHistory = new ArrayList<>();
  final Stack<Frame> stack = new Stack<>();
  final AtomicInteger aliasInt = new AtomicInteger(0);


  public TablePathBuilder(CatalogResolver catalogResolver, Map<FunctionParameter, SqlDynamicParam> paramMapping,
      AtomicInteger uniquePkId) {
    this.catalogResolver = catalogResolver;
    this.paramMapping = paramMapping;
    this.uniquePkId = uniquePkId;
  }

  public Result build(List<SqlNode> items, Context context, List<FunctionParameter> parameters) {
    List<FunctionParameter> params = new ArrayList<>(parameters);
    Iterator<SqlNode> input = items.iterator();
    PathWalker pathWalker = new PathWalker(catalogResolver);
    List<PullupColumn> pullupColumns = processFirstItem(input, pathWalker, context, params);
    processRemainingItems(input, pathWalker, context, params);
    SqlNode sqlNode = buildAndProjectLast(pullupColumns);
    return createResult(sqlNode, pathWalker.getAbsolutePath(), pullupColumns, params);
  }

  private List<PullupColumn> processFirstItem(Iterator<SqlNode> input, PathWalker pathWalker,
      Context context, List<FunctionParameter> params) {
    SqlNode item = input.next();
    String identifier = getIdentifier(item)
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

    if (catalogResolver.getTableFunction(List.of(identifier)).isPresent()) {
      pathWalker.walk(identifier);
      scanFunction(pathWalker.getPath(), List.of());
    } else if (context.hasAlias(identifier)) {
      handleAlias(input, pathWalker, context, identifier, params);
    } else if (identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return handleSelf(input, pathWalker, context, params);
    } else {
      throw new RuntimeException("Unknown table: " + item);
    }

    return List.of();
  }

  private void handleAlias(Iterator<SqlNode> input, PathWalker pathWalker, Context context, String identifier,
      List<FunctionParameter> params) {
    if (!input.hasNext()) {
      throw new RuntimeException("Alias by itself.");
    }

    pathWalker.setPath(context.getAliasPath(identifier));
    String nextIdentifier = getIdentifier(input.next())
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
    pathWalker.walk(nextIdentifier);

    Optional<SqlUserDefinedTableFunction> fnc = catalogResolver.getTableFunction(pathWalker.getPath());
    Preconditions.checkState(fnc.isPresent(), "Table function not found %s", pathWalker.getPath());
    List<SqlNode> args = rewriteArgs(identifier, fnc.get().getFunction(), context, params);
    scanFunction(fnc.get(), args);
  }

  private List<PullupColumn> handleSelf(Iterator<SqlNode> input, PathWalker pathWalker, Context context,
      List<FunctionParameter> params) {
    pathWalker.setPath(context.getCurrentPath());
    boolean isNested = context.isNested();
    boolean materializeSelf = context.isMaterializeSelf();

    if (materializeSelf || !input.hasNext()) {
      scanNestedTable(context.getCurrentPath());
      if (isNested) {
        return buildPullupColumns(pathWalker);
      }
    } else {
      String nextIdentifier = getIdentifier(input.next())
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      Optional<SqlUserDefinedTableFunction> fnc = catalogResolver.getTableFunction(pathWalker.getAbsolutePath());
      Preconditions.checkState(fnc.isPresent(), "Table function not found %s", pathWalker.getPath());
      List<SqlNode> args = rewriteArgs(ReservedName.SELF_IDENTIFIER.getCanonical(), fnc.get().getFunction(), context,
          params);
      scanFunction(fnc.get(), args);
    }
    return List.of();
  }

  private List<PullupColumn> buildPullupColumns(PathWalker pathWalker) {
    RelOptTable table = catalogResolver.getTableFromPath(pathWalker.getAbsolutePath());
    return IntStream.range(0, table.getKeys().get(0).asSet().size())
        .mapToObj(i -> new PullupColumn(
            String.format("%spk%d$%s", ReservedName.SYSTEM_HIDDEN_PREFIX, uniquePkId.incrementAndGet(), table.getRowType().getFieldList().get(i).getName()),
            String.format("%spk%d$%s", ReservedName.SYSTEM_HIDDEN_PREFIX, i + 1, table.getRowType().getFieldList().get(i).getName())
        ))
        .collect(Collectors.toList());
  }

  private void processRemainingItems(Iterator<SqlNode> input, PathWalker pathWalker,
      Context context, List<FunctionParameter> params) {

    while (input.hasNext()) {
      SqlNode item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      String alias = getLatestAlias();
      Optional<SqlUserDefinedTableFunction> fnc = catalogResolver.getTableFunction(pathWalker.getPath());
      if (fnc.isEmpty()) {
        scanNestedTable(pathWalker.getPath());
      } else {
        List<SqlNode> args = rewriteArgs(alias, fnc.get().getFunction(), context,
            params);
        scanFunction(fnc.get(), args);
        joinLateral();
      }
    }
  }

  private Result createResult(SqlNode sqlNode, List<String> path, List<PullupColumn> pullupColumns,
      List<FunctionParameter> params) {
    return new Result(sqlNode, path, pullupColumns, List.of(), Optional.empty(), params);
  }

  private List<SqlNode> rewriteArgs(String alias, TableFunction function, Context context,
      List<FunctionParameter> params) {
    //if arg needs to by a dynamic expression, rewrite.
    List<SqlNode> nodes = new ArrayList<>();
    for (FunctionParameter parameter : function.getParameters()) {
      SqrlFunctionParameter p = (SqrlFunctionParameter) parameter;
      SqlIdentifier identifier = new SqlIdentifier(List.of(alias, p.getName()),
          SqlParserPos.ZERO);
      SqlNode rewritten = context.isMaterializeSelf()
          ? identifier
          : rewriteToDynamicParam(identifier, params);
      nodes.add(rewritten);
    }
    return nodes;
  }

  public SqlNode rewriteToDynamicParam(SqlIdentifier id, List<FunctionParameter> params) {
    //if self, check if param list, if not create one
    if (!isSelfField(id.names)) {
      return id;
    }

    for (FunctionParameter p : params) {
      if (paramMapping.containsKey(p)) {
        return paramMapping.get(p);
      }
    }

    RelDataType anyType = catalogResolver.getTypeFactory().createSqlType(SqlTypeName.ANY);
    SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(id.names.get(1), Optional.empty(),
        SqlDataTypeSpecBuilder.create(anyType), params.size(), anyType, true);
    params.add(functionParameter);
    SqlDynamicParam param = new SqlDynamicParam(functionParameter.getOrdinal(),
        id.getParserPosition());
    paramMapping.put(functionParameter, param);

    return param;
  }

  private Optional<String> getIdentifier(SqlNode item) {
    if (item instanceof SqlIdentifier) {
      return Optional.of(((SqlIdentifier) item).getSimple());
    } else if (item instanceof SqlCall) {
      return Optional.of(((SqlCall) item).getOperator().getName());
    }

    return Optional.empty();
  }

  private String generateAlias() {
    return ALIAS_PREFIX + aliasInt.incrementAndGet();
  }
  public void scanFunction(List<String> path, List<SqlNode> args) {
    Optional<SqlUserDefinedTableFunction> op = catalogResolver.getTableFunction(path);
    if (op.isEmpty() && args.isEmpty()) {
      scanNestedTable(path);
      return;
    }
    if (op.isEmpty()) {
      throw new RuntimeException(String.format("Could not find table: %s", path));
    }

    scanFunction(op.get(), args);
  }

  public void scanFunction(SqlUserDefinedTableFunction op,
      List<SqlNode> args) {
    RelDataType type = op.getFunction().getRowType(null, null);
    SqlCall call = op.createCall(SqlParserPos.ZERO, args);

    call = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, call);

    String alias = generateAlias();
    SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, call, new SqlIdentifier(alias, SqlParserPos.ZERO));
    Frame frame = new Frame(false, type, aliasedCall, alias);
    stack.push(frame);
    tableHistory.add(frame);
  }

  public void joinLateral() {
    Frame right = stack.pop();
    Frame left = stack.pop();

    SqlJoin join = new SqlJoin(SqlParserPos.ZERO,
        left.getNode(),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
        SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, right.getNode()),
        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
        null);

    RelDataTypeFieldBuilder builder = new RelDataTypeFieldBuilder(
        new FieldInfoBuilder(catalogResolver.getTypeFactory()));
    builder.addAll(left.getType().getFieldList());
    builder.addAll(right.getType().getFieldList());
    RelDataType type = builder.build();

    Frame frame = new Frame(right.subquery, type, join, right.getAlias());
    stack.push(frame);
  }

  public SqlNode build() {
    Frame frame = stack.pop();
    return frame.getNode();
  }
  public SqlNode buildAndProjectLast(List<PullupColumn> pullupCols) {
    Frame frame = stack.pop();
    Frame lastTable = tableHistory.get(tableHistory.size()-1);
    if (frame.isSubquery()) { //subquery
      return frame.getNode();
    }
    SqlSelectBuilder select = new SqlSelectBuilder()
        .setFrom(frame.getNode());
    select.setSelectList(ListUtils.union(
        rename(createSelectList(tableHistory.get(0), pullupCols.size()), pullupCols),
        createSelectList(lastTable, lastTable.type.getFieldCount())));
    return select.build();
  }

  private List rename(List<SqlIdentifier> selectList, List<PullupColumn> pullupCols) {
    return IntStream.range(0, selectList.size())
        .mapToObj(i-> SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, selectList.get(i),
            new SqlIdentifier(pullupCols.get(i).getColumnName(), SqlParserPos.ZERO)))
        .collect(Collectors.toList());
  }

  private List<SqlIdentifier> createSelectList(Frame frame, int count) {
    return IntStream.range(0, count)
        .mapToObj(i->
            //todo fix: alias has null check for no alias on subquery
            (frame.alias == null)
                ? new SqlIdentifier(List.of( frame.getType().getFieldList().get(i).getName()), SqlParserPos.ZERO )
                : new SqlIdentifier(List.of(frame.alias, frame.getType().getFieldList().get(i).getName()), SqlParserPos.ZERO )
        )
        .collect(Collectors.toList());
  }

  public String getLatestAlias() {
    return tableHistory.get(tableHistory.size()-1).alias;
  }

  public void scanNestedTable(List<String> currentPath) {
    RelOptTable relOptTable = catalogResolver.getTableFromPath(currentPath);
    if (relOptTable == null) {
      throw new RuntimeException("Could not find table: " + currentPath);
    }
    String tableName = relOptTable.getQualifiedName().get(0);
    SqlNode table = new SqlIdentifier(tableName, SqlParserPos.ZERO);

    String alias = "_t"+aliasInt.incrementAndGet();

    SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, table, new SqlIdentifier(alias, SqlParserPos.ZERO));
    Frame frame = new Frame(false, relOptTable.getRowType(), aliasedCall, alias);
    stack.push(frame);
    tableHistory.add(frame);
  }

  @Value
  public class Frame {
    boolean subquery;
    RelDataType type;
    SqlNode node;
    String alias;
  }
}
