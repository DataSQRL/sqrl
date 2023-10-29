package com.datasqrl.calcite;

import static com.datasqrl.plan.validate.ScriptValidator.isSelfField;

import com.datasqrl.calcite.SqrlToSql.Context;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.sqrl.CatalogResolver;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlFunctionParameter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

/**
 * Converts a table path to a normalized representation.
 */
public class NormalizeTablePath {
  private static final String ALIAS_PREFIX = "_t";
  private final CatalogReader catalogResolver;
  private final Map<FunctionParameter, SqlDynamicParam> paramMapping;
  private final AtomicInteger aliasInt = new AtomicInteger(0);

  public NormalizeTablePath(CatalogReader catalogResolver, Map<FunctionParameter, SqlDynamicParam> paramMapping) {
    this.catalogResolver = catalogResolver;
    this.paramMapping = paramMapping;
  }

  public TablePathResult convert(List<SqlNode> items, Context context, List<FunctionParameter> parameters) {
    // Map items to higher level objects, then walk the objects
    List<FunctionParameter> params = new ArrayList<>(parameters);
    PathWalker pathWalker = new PathWalker(catalogResolver);
    List<PathItem> pathItems = mapToPathItems(context, items, params, pathWalker);
    return new TablePathResult(params, pathItems, pathWalker.getAbsolutePath());
  }

  private List<PathItem> mapToPathItems(Context context, List<SqlNode> items, List<FunctionParameter> params,
      PathWalker pathWalker) {
    List<PathItem> pathItems = new ArrayList<>();
    Iterator<SqlNode> input = items.iterator();

    SqlNode item = input.next();
    String identifier = getIdentifier(item)
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

    String alias;
    if (getTable(identifier).isPresent()) {
      TableFunction op = getTable(identifier).get();
      alias = generateAlias();
      pathItems.add(new TableFunctionPathItem(pathWalker.getPath(), op, List.of(), alias));
      pathWalker.walk(identifier);
    } else if (context.hasAlias(identifier)) {
      // For tables that start with an alias e.g. `o.entries`
      if (!input.hasNext()) {
        throw new RuntimeException("Alias by itself.");
      }
      alias = generateAlias();
      // Get absolute path of alias `o`
      pathWalker.setPath(context.getAliasPath(identifier));
      String nextIdentifier = getIdentifier(input.next())
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      // Update path walker
      pathWalker.walk(nextIdentifier);

      // Lookup function
      Optional<TableFunction> fnc = getTable(pathWalker.getPath());
      Preconditions.checkState(fnc.isPresent(), "Table function not found %s", pathWalker.getPath());

      // Rewrite arguments so internal arguments are prefixed with the alias
      List<SqlNode> args = rewriteArgs(identifier, fnc.get(), context, params);

      pathItems.add(new TableFunctionPathItem(pathWalker.getPath(), fnc.get(), args, alias));
    } else if (identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      //Tables that start with '@'
      pathWalker.setPath(context.getCurrentPath());
      // Treat '@' as something to add to the table path list
      boolean materializeSelf = context.isMaterializeSelf();

      if (materializeSelf || !input.hasNext()) {
        // Do a table scan on the source table
        RelOptTable table = catalogResolver.getTableFromPath(pathWalker.getPath());
        pathItems.add(new SelfTablePathItem(pathWalker.getPath(), table));
        alias = ReservedName.SELF_IDENTIFIER.getCanonical();
      } else {
        String nextIdentifier = getIdentifier(input.next())
            .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
        pathWalker.walk(nextIdentifier);

        Optional<TableFunction> fnc = getTable(pathWalker.getAbsolutePath());
        Preconditions.checkState(fnc.isPresent(), "Table function not found %s", pathWalker.getPath());

        alias = generateAlias();
        // Rewrite arguments
        List<SqlNode> args = rewriteArgs(ReservedName.SELF_IDENTIFIER.getCanonical(), fnc.get(), context,
            params);
        pathItems.add(new TableFunctionPathItem(pathWalker.getPath(), fnc.get(), args, alias));
      }
    } else {
      throw new RuntimeException("Unknown table: " + item);
    }

    while (input.hasNext()) {
      item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);


      Optional<TableFunction> fnc = getTable(pathWalker.getPath());
      List<SqlNode> args = rewriteArgs(alias, fnc.get(), context, params);
      String newAlias = generateAlias();
      pathItems.add(new TableFunctionPathItem(pathWalker.getPath(), fnc.get(), args, newAlias));
      alias = newAlias;
    }

    return pathItems;
  }

  private Optional<TableFunction> getTable(List<String> path) {
    return getTable(String.join(".", path));
  }

  private Optional<TableFunction> getTable(String identifier) {
    ArrayList<Function> functions = new ArrayList<>(
        catalogResolver.getSchema().getFunctions(identifier, false));
    //first function assumed to be our table function
    // Also assumed to be table function
    //TODO: Fix me
    return functions.isEmpty() ? Optional.empty() : Optional.of((TableFunction) functions.get(0));
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


  public interface PathItem {
    String getAlias();
  }

  @AllArgsConstructor
  @Getter
  public class TableFunctionPathItem implements PathItem {
    List<String> path;
    TableFunction op;
    List<SqlNode> arguments;
    String alias;
  }

  @AllArgsConstructor
  @Getter
  public class SelfTablePathItem implements PathItem {
    List<String> path;
    RelOptTable table;

    @Override
    public String getAlias() {
      return ReservedName.SELF_IDENTIFIER.getCanonical();
    }
  }

  @Value
  public class TablePathResult {
    List<FunctionParameter> params;
    List<PathItem> pathItems;
    List<String> path;
  }
}
