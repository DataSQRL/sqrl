package com.datasqrl.calcite;

import static com.datasqrl.plan.validate.ScriptPlanner.addError;
import static com.datasqrl.plan.validate.ScriptPlanner.isSelfField;

import com.datasqrl.calcite.SqrlToSql.Context;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.function.SqrlFunctionParameter.UnknownCaseParameter;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Converts a table path to a normalized representation.
 */
public class NormalizeTablePath {
  private static final String ALIAS_PREFIX = "_t";
  private final CatalogReader catalogResolver;
  private final Map<FunctionParameter, SqlDynamicParam> paramMapping;
  private final AtomicInteger aliasInt = new AtomicInteger(0);
  private final SqlNameUtil nameUtil;
  private final ErrorCollector errors;

  public NormalizeTablePath(CatalogReader catalogReader, Map<FunctionParameter, SqlDynamicParam> paramMapping,
      SqlNameUtil nameUtil, ErrorCollector errors) {
    this.catalogResolver = catalogReader;
    this.paramMapping = paramMapping;
    this.nameUtil = nameUtil;
    this.errors = errors;
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
    Name identifier = getIdentifier(item)
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

    Name alias;
    if (getTable(identifier).isPresent()) {
      alias = generateAlias();
      pathItems.add(new TableFunctionPathItem(identifier, List.of(), alias));
      pathWalker.walk(identifier);
    } else if (context.hasAlias(identifier)) {
      // For tables that start with an alias e.g. `o.entries`
      if (!input.hasNext()) {
        throw addError(errors, ErrorLabel.GENERIC, item, "Alias by itself.");
      }
      alias = generateAlias();
      // Get absolute path of alias `o`
      NamePath path = context.getAliasPath(identifier)
          .orElseThrow(() -> new RuntimeException("Could not find alias: " + identifier));
      pathWalker.setPath(path);
      Name nextIdentifier = getIdentifier(input.next())
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      SqrlTableMacro parent = getTable(pathWalker.getPath()).get();

      // Update path walker
      pathWalker.walk(nextIdentifier);

      // Lookup function
      Optional<SqrlTableMacro> fnc = getTable(pathWalker.getPath());
      boolean hasTableFnc = hasTableFunction(pathWalker.getPath());
      Preconditions.checkState(hasTableFnc, "Could not find table: %s", pathWalker.getPath());

      List<SqrlFunctionParameter> internalParams = getInternalParamsOfTable(pathWalker.getPath());
      // Rewrite arguments so internal arguments are prefixed with the alias
      List<SqlNode> args = rewriteInternalArgs(identifier, internalParams, context, params, parent.getRowType());

      pathItems.add(new TableFunctionPathItem(
          nameUtil.toName(pathWalker.getPath().getDisplay()), args, alias));
    } else if (identifier.equals(ReservedName.SELF_IDENTIFIER)) {
      //Tables that start with '@'
      pathWalker.setPath(context.getCurrentPath());
      // Treat '@' as something to add to the table path list
      boolean materializeSelf = context.isMaterializeSelf();

      if (materializeSelf || !input.hasNext()) {
        // Do a table scan on the source table
        Optional<RelOptTable> table = catalogResolver.getTableFromPath(pathWalker.getPath());
        if (table.isEmpty()) {
          throw addError(errors, ErrorLabel.GENERIC, item, "Could not find path item at: %s",
              pathWalker.getPath().getDisplay());
        }
        table.map(t->pathItems.add(new SelfTablePathItem(t)));
        alias = ReservedName.SELF_IDENTIFIER;
      } else {
        Name nextIdentifier = getIdentifier(input.next())
            .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
        SqrlTableMacro parentTable = getTable(pathWalker.getAbsolutePath()).get();

        pathWalker.walk(nextIdentifier);

        boolean hasTableFnc = hasTableFunction(pathWalker.getPath());
        Preconditions.checkState(hasTableFnc, "Could not find table: %s", pathWalker.getPath());

        List<SqrlFunctionParameter> internalParams = getInternalParamsOfTable(pathWalker.getPath());

        alias = generateAlias();
        // Rewrite arguments
        List<SqlNode> args = rewriteInternalArgs(ReservedName.SELF_IDENTIFIER, internalParams, context,
            params, parentTable.getRowType());
        pathItems.add(new TableFunctionPathItem(
            nameUtil.toName(pathWalker.getPath().getDisplay()), args, alias));
      }
    } else {
      throw addError(errors, ErrorLabel.GENERIC, item,
          "Could not find table: %s", identifier);
    }

    while (input.hasNext()) {
      item = input.next();
      SqrlTableMacro parentTable = getTable(pathWalker.getAbsolutePath()).get();
      SqlNode finalNode = item;
      Name nextIdentifier = getIdentifier(item)
          .orElseThrow(() -> addError(errors, ErrorLabel.GENERIC, finalNode, "Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      boolean hasTableFnc = hasTableFunction(pathWalker.getPath());
      if (!hasTableFnc) {
        throw addError(errors, ErrorLabel.GENERIC, item, "Could not find table: %s", pathWalker.getPath());
      }
      List<SqrlFunctionParameter> internalParams = getInternalParamsOfTable(pathWalker.getPath());

      List<SqlNode> args = rewriteInternalArgs(alias, internalParams, context, params,
          parentTable.getRowType());
      Name newAlias = generateAlias();
      pathItems.add(new TableFunctionPathItem(
          nameUtil.toName(pathWalker.getPath().getDisplay()), args, newAlias));
      alias = newAlias;
    }

    return pathItems;
  }

  private boolean hasTableFunction(NamePath path) {
    return getTable(path).isPresent();
  }

  // Internal params are all the same for a given path
  private List<SqrlFunctionParameter> getInternalParamsOfTable(NamePath path) {
    return getTable(path)
        .get().getParameters().stream()
        .map(p->(SqrlFunctionParameter)p)
        .filter(SqrlFunctionParameter::isInternal)
        .collect(Collectors.toList());
  }

  private Optional<SqrlTableMacro> getTable(Name name) {
    return getTable(name.toNamePath());
  }

  private Optional<SqrlTableMacro> getTable(NamePath path) {
    return getTable(path.getDisplay());
  }

  private Optional<SqrlTableMacro> getTable(String identifier) {
    ArrayList<Function> functions = new ArrayList<>(
        catalogResolver.getSchema().getFunctions(identifier, false));
    //Get first function, calcite will resolve parameters
    return functions.isEmpty() ? Optional.empty() : Optional.of((SqrlTableMacro) functions.get(0));
  }

  private List<SqlNode> rewriteInternalArgs(Name alias, List<SqrlFunctionParameter> internalParams, Context context,
      List<FunctionParameter> params, RelDataType parentRowType) {
    List<SqlNode> nodes = new ArrayList<>();
    for (SqrlFunctionParameter param : internalParams) {
      Optional<String> parentParameterName = param.getParentName()
          .resolve(parentRowType, catalogResolver.nameMatcher());
      if (parentParameterName.isEmpty()) {
        throw new RuntimeException("Internal error, cannot find parent parameter name: " + param.getVariableName());
      }
      SqlIdentifier identifier = new SqlIdentifier(List.of(alias.getDisplay(),parentParameterName.get()),
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
    if (!isSelfField(nameUtil.toNamePath(id.names))) {
      return id;
    }

    for (FunctionParameter p : params) {
      if (paramMapping.containsKey(p)) {
        return paramMapping.get(p);
      }
    }

    RelDataType anyType = catalogResolver.getTypeFactory().createSqlType(SqlTypeName.ANY);
    SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(id.names.get(1), Optional.empty(),
        SqlDataTypeSpecBuilder.create(anyType), params.size(), anyType, true,
        new UnknownCaseParameter(id.names.get(1)));
    params.add(functionParameter);
    SqlDynamicParam param = new SqlDynamicParam(functionParameter.getOrdinal(),
        id.getParserPosition());
    paramMapping.put(functionParameter, param);

    return param;
  }

  private Optional<Name> getIdentifier(SqlNode item) {
    if (item instanceof SqlIdentifier) {
      return Optional.of(((SqlIdentifier) item).getSimple())
          .map(nameUtil::toName);
    }

    return Optional.empty();
  }

  private Name generateAlias() {
    return nameUtil.toName(ALIAS_PREFIX + aliasInt.incrementAndGet());
  }


  public interface PathItem {
    Name getAlias();
  }

  @AllArgsConstructor
  @Getter
  public class TableFunctionPathItem implements PathItem {
    Name functionName;
    List<SqlNode> arguments;
    Name alias;
  }

  @AllArgsConstructor
  @Getter
  public class SelfTablePathItem implements PathItem {
    RelOptTable table;

    @Override
    public Name getAlias() {
      return ReservedName.SELF_IDENTIFIER;
    }
  }

  @Value
  public class TablePathResult {
    List<FunctionParameter> params;
    List<PathItem> pathItems;
    NamePath path;
  }
}
