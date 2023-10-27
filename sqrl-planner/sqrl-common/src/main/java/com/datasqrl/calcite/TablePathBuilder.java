package com.datasqrl.calcite;

import static com.datasqrl.plan.validate.ScriptValidator.isSelfField;

import com.datasqrl.calcite.SqrlToSql.Context;
import com.datasqrl.calcite.SqrlToSql.PullupColumn;
import com.datasqrl.calcite.SqrlToSql.Result;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.sql.SqlJoinPathBuilder;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlFunctionParameter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class TablePathBuilder {

  private final CatalogReader catalogReader;
  private final TypeFactory typeFactory;
  private final Map<FunctionParameter, SqlDynamicParam> paramMapping;
  private final AtomicInteger uniquePkId;

  public TablePathBuilder(CatalogReader catalogReader, TypeFactory typeFactory,Map<FunctionParameter, SqlDynamicParam> paramMapping,
      AtomicInteger uniquePkId) {
    this.catalogReader = catalogReader;
    this.typeFactory = typeFactory;
    this.paramMapping = paramMapping;
    this.uniquePkId = uniquePkId;
  }

  public Result build(List<SqlNode> items, Context context, List<FunctionParameter> parameters) {
    List<FunctionParameter> params = new ArrayList<>(parameters);

    Iterator<SqlNode> input = items.iterator();
    PathWalker pathWalker = new PathWalker(catalogReader);
    SqlJoinPathBuilder builder = new SqlJoinPathBuilder(catalogReader);

    List<PullupColumn> pullupColumns = processFirstItem(input, pathWalker, builder, context, params);

    processRemainingItems(input, pathWalker, builder, context, params);

    SqlNode sqlNode = builder.buildAndProjectLast(pullupColumns);

    return createResult(sqlNode, pathWalker.getAbsolutePath(), pullupColumns, params);
  }

  private List<PullupColumn> processFirstItem(Iterator<SqlNode> input, PathWalker pathWalker,
      SqlJoinPathBuilder builder, Context context, List<FunctionParameter> params) {
    SqlNode item = input.next();
    String identifier = getIdentifier(item)
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

    if (catalogReader.getTableFunction(List.of(identifier)).isPresent()) {
      pathWalker.walk(identifier);
      builder.scanFunction(pathWalker.getPath(), List.of());
    } else if (context.hasAlias(identifier)) {
      handleAlias(input, pathWalker, builder, context, identifier, params);
    } else if (identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return handleSelf(input, pathWalker, builder, context, params);
    } else {
      throw new RuntimeException("Unknown table: " + item);
    }

    return List.of();
  }

  private void handleAlias(Iterator<SqlNode> input, PathWalker pathWalker, SqlJoinPathBuilder builder, Context context, String identifier,
      List<FunctionParameter> params) {
    if (!input.hasNext()) {
      throw new RuntimeException("Alias by itself.");
    }

    pathWalker.setPath(context.getAliasPath(identifier));
    String nextIdentifier = getIdentifier(input.next())
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
    pathWalker.walk(nextIdentifier);

    Optional<SqlUserDefinedTableFunction> fnc = catalogReader.getTableFunction(pathWalker.getPath());
    Preconditions.checkState(fnc.isPresent(), "Table function not found %s", pathWalker.getPath());
    List<SqlNode> args = rewriteArgs(identifier, fnc.get().getFunction(), context, params);
    builder.scanFunction(fnc.get(), args);
  }

  private List<PullupColumn> handleSelf(Iterator<SqlNode> input, PathWalker pathWalker, SqlJoinPathBuilder builder, Context context,
      List<FunctionParameter> params) {
    pathWalker.setPath(context.getCurrentPath());
    boolean isNested = context.isNested();
    boolean materializeSelf = context.isMaterializeSelf();

    if (materializeSelf || !input.hasNext()) {
      builder.scanNestedTable(context.getCurrentPath());
      if (isNested) {
        return buildPullupColumns(pathWalker);
      }
    } else {
      String nextIdentifier = getIdentifier(input.next())
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      Optional<SqlUserDefinedTableFunction> fnc = catalogReader.getTableFunction(pathWalker.getAbsolutePath());
      Preconditions.checkState(fnc.isPresent(), "Table function not found %s", pathWalker.getPath());
      List<SqlNode> args = rewriteArgs(ReservedName.SELF_IDENTIFIER.getCanonical(), fnc.get().getFunction(), context,
          params);
      builder.scanFunction(fnc.get(), args);
    }
    return List.of();
  }

  private List<PullupColumn> buildPullupColumns(PathWalker pathWalker) {
    RelOptTable table = catalogReader.getTableFromPath(pathWalker.getAbsolutePath());
    return IntStream.range(0, table.getKeys().get(0).asSet().size())
        .mapToObj(i -> new PullupColumn(
            String.format("%spk%d$%s", ReservedName.SYSTEM_HIDDEN_PREFIX, uniquePkId.incrementAndGet(), table.getRowType().getFieldList().get(i).getName()),
            String.format("%spk%d$%s", ReservedName.SYSTEM_HIDDEN_PREFIX, i + 1, table.getRowType().getFieldList().get(i).getName())
        ))
        .collect(Collectors.toList());
  }

  private void processRemainingItems(Iterator<SqlNode> input, PathWalker pathWalker,
      SqlJoinPathBuilder builder, Context context, List<FunctionParameter> params) {

    while (input.hasNext()) {
      SqlNode item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      String alias = builder.getLatestAlias();
      Optional<SqlUserDefinedTableFunction> fnc = catalogReader.getTableFunction(pathWalker.getPath());
      if (fnc.isEmpty()) {
        builder.scanNestedTable(pathWalker.getPath());
      } else {
        List<SqlNode> args = rewriteArgs(alias, fnc.get().getFunction(), context,
            params);
        builder.scanFunction(fnc.get(), args)
            .joinLateral();
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

    RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(id.names.get(1),
        Optional.empty(), SqlDataTypeSpecBuilder
        .create(anyType), params.size(), anyType,
        true);
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
}
