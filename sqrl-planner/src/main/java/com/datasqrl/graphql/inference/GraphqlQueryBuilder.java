package com.datasqrl.graphql.inference;

import static com.datasqrl.canonicalizer.ReservedName.VARIABLE_PREFIX;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator.ArgCombination;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class GraphqlQueryBuilder {

  private final AtomicInteger queryCounter = new AtomicInteger();

  SqrlFramework framework;
  APIConnectorManager apiManager;
  SqlNameUtil nameUtil;

  public APIQuery create(List<ArgCombination> arg, SqrlTableMacro macro,
      String parentName, FieldDefinition fieldDefinition, RelDataType parentType) {

    Pair<SqlUserDefinedTableFunction, Boolean> operatorPair = resolveOperator(macro, arg);
    SqlUserDefinedTableFunction operator = operatorPair.getLeft();
    boolean allowPermutation = operatorPair.getRight();

    String nameId = generateQueryNameId(parentName, fieldDefinition);

    QueryBuilderHelper queryBuilderHelper = new QueryBuilderHelper(framework.getQueryPlanner(),
        framework.getQueryPlanner().getRelBuilder(),
        nameId);

    if (allowPermutation) {
      for (SqrlFunctionParameter parameter : getInternalParams(operator.getFunction().getParameters())) {
        Optional<String> parentFieldName = parameter.getParentName()
            .resolve(parentType, framework.getCatalogReader().nameMatcher());
        if (parentFieldName.isEmpty()) {
          throw new RuntimeException("Could not find parameter: " + parameter.getVariableName());
        }
        queryBuilderHelper.addInternalOperand(parentFieldName.get(), parameter.getRelDataType());
      }
      queryBuilderHelper.scan(operator);

      for (ArgCombination c : arg) {
        if (isLimitOrOffset(c)) {
          continue;
        }

        String columnToFilter = c.getDefinition().getName();
        RelDataType columnType = graphqlToRelDataType(c.getDefinition().getType(), framework.getTypeFactory());
        queryBuilderHelper.filter(columnToFilter, columnType);
      }
    } else {
      ImmutableMap<Name, ArgCombination> nameToArg = Maps.uniqueIndex(arg,
          a -> nameUtil.toName(a.getDefinition().getName().toLowerCase()));

      // Iterate over the table function to resolve all parameters it needs
      for (FunctionParameter functionParameter : operator.getFunction().getParameters()) {
        SqrlFunctionParameter parameter = (SqrlFunctionParameter) functionParameter;
        if (parameter.isInternal()) {
          Optional<String> parentFieldName = parameter.getParentName()
              .resolve(parentType, framework.getCatalogReader().nameMatcher());
          if (parentFieldName.isEmpty()) {
            throw new RuntimeException("Could not find parameter: " + parameter.getVariableName());
          }
          queryBuilderHelper.addInternalOperand(parentFieldName.get(), parameter.getRelDataType());
        } else {
          ArgCombination tableArgument = nameToArg.get(nameUtil.toName(parameter.getVariableName()));
          if (tableArgument == null) {
            throw new RuntimeException("Could not find argument: " + parameter.getVariableName());
          }
          String operand = tableArgument.getDefinition().getName();
          RelDataType operandType = graphqlToRelDataType(tableArgument.getDefinition().getType(), framework.getTypeFactory());

          queryBuilderHelper.addVariableOperand(operand, operandType);
        }
      }
      queryBuilderHelper.scan(operator);
    }

    queryBuilderHelper.applyExtraFilters();

    Optional<ArgCombination> limit = onlyLimit(arg);
    Optional<ArgCombination> offset = onlyOffset(arg);
    if (limit.isPresent() || offset.isPresent()) {
      queryBuilderHelper.limitOffset(limit, offset);
    }

    APIQuery query = queryBuilderHelper.build(macro.getFullPath());

    //Validate all source args are resolvable
    List<SourceParameter> sourceParams = queryBuilderHelper.graphqlArguments.stream()
        .filter(f -> f instanceof SourceParameter)
        .map(f -> (SourceParameter) f)
        .collect(Collectors.toList());

    if (parentType != null) {
      Set<String> parentRowType = new HashSet<>(parentType.getFieldNames());
      for (SourceParameter p : sourceParams) {
        if (!parentRowType.contains(p.getKey())) {
          throw new RuntimeException("Could not correctly create query");
        }
      }
    } else {
      Preconditions.checkState(sourceParams.isEmpty(), "Expected no source params");
    }

    return query;
  }

  private String generateQueryNameId(String parentName, FieldDefinition fieldDefinition) {
    return parentName + "." + fieldDefinition.getName() + "-"
        + queryCounter.incrementAndGet();
  }

  private boolean isLimitOrOffset(ArgCombination c) {
    return c.getDefinition().getName().equalsIgnoreCase(LIMIT) ||
        c.getDefinition().getName().equalsIgnoreCase(OFFSET);
  }

  private Pair<SqlUserDefinedTableFunction, Boolean> resolveOperator(SqrlTableMacro macro, List<ArgCombination> arg) {
    // Check for permutation case and bail early. It's costly to check the operands for all cases.
    List<Function> functions = new ArrayList<>(framework.getSchema()
        .getFunctions(macro.getDisplayName(), false));
    if (functions.size() == 1 && getExternalParams(functions.get(0).getParameters()).size() == 0) {
      List<SqlOperator> operators = getOperators(macro.getDisplayName(), List.of(),
          getInternalParams(functions.get(0).getParameters()));
      if (operators.isEmpty()) {
        throw new RuntimeException(String.format(
            "Could not find operator: %s(%s)", macro.getDisplayName(), getInternalParams(functions.get(0).getParameters()).stream()
                .map(SqrlFunctionParameter::getName)
                .collect(Collectors.joining(","))));
      } else if (operators.size() > 1) {
        throw new RuntimeException(String.format(
            "Ambiguous operator: %s", macro.getDisplayName()));
      }
      return Pair.of((SqlUserDefinedTableFunction) Iterables.getOnlyElement(operators), true);
    }

    // Lookup operator with full arguments. If not found, look for table fnc with no args and allow permutation
    List<ArgCombination> pagingRemoved = removePaging(arg);
    List<SqrlFunctionParameter> internalParams = getInternalParams(macro.getParameters());
    String tableFunctionName = macro.getFullPath().getDisplay();

    // Look for function with all args
    List<SqlOperator> operators = getOperators(tableFunctionName, pagingRemoved, internalParams);
    if (operators.size() == 1) {
      return Pair.of((SqlUserDefinedTableFunction)operators.get(0), false);
    }
    if (operators.size() > 1) {
      throw new RuntimeException("Expected exactly one matching table function for '"+tableFunctionName+"', found: " + operators.size());
    }

    // Look for function with no args
    operators = getOperators(tableFunctionName, List.of(), internalParams);
    if (operators.size() == 0) {
      throw new RuntimeException("Could not find function for '"+tableFunctionName+"'");
    } else if (operators.size() > 1) {
      throw new RuntimeException("Expected exactly one matching table function for '"+tableFunctionName+"', found: " + operators.size());
    } else {
      return Pair.of((SqlUserDefinedTableFunction)operators.get(0), true);
    }
  }

  private List<SqrlFunctionParameter> getExternalParams(List<FunctionParameter> params) {
    return params.stream()
        .map(p -> (SqrlFunctionParameter) p)
        .filter(p -> !p.isInternal())
        .collect(Collectors.toList());
  }

  private List<SqrlFunctionParameter> getInternalParams(List<FunctionParameter> params) {
    return params.stream()
        .map(p -> (SqrlFunctionParameter) p)
        .filter(SqrlFunctionParameter::isInternal)
        .collect(Collectors.toList());
  }

  private Optional<ArgCombination> onlyLimit(List<ArgCombination> arg) {
    return arg.stream()
        .filter(f -> f.getDefinition().getName().equalsIgnoreCase(LIMIT))
        .findFirst();
  }

  private Optional<ArgCombination> onlyOffset(List<ArgCombination> arg) {
    return arg.stream()
        .filter(f -> f.getDefinition().getName().equalsIgnoreCase(OFFSET))
        .findFirst();
  }

  private List<ArgCombination> removePaging(List<ArgCombination> arg) {
    return arg.stream()
        .filter(f -> !f.getDefinition().getName().equalsIgnoreCase(LIMIT)
            && !f.getDefinition().getName().equalsIgnoreCase(OFFSET))
        .collect(Collectors.toList());
  }

  private List<SqlOperator> getOperators(String name, List<ArgCombination> arg,
      List<SqrlFunctionParameter> internal) {
    List<RelDataType> argsTypes = constructArgTypes(arg, framework.getTypeFactory());
    internal.forEach(p -> argsTypes.add(p.getRelDataType()));

    List<String> argNames = constructArgNames(arg);
    internal.forEach(p -> argNames.add(p.getName()));

    SqlIdentifier nameIdentifier = new SqlIdentifier(name, SqlParserPos.ZERO);
    Iterator<SqlOperator> sqlOperatorIterator = SqlUtil.lookupSubjectRoutines(
        framework.getSqrlOperatorTable(), framework.getTypeFactory(),
        nameIdentifier, argsTypes, argNames, SqlSyntax.FUNCTION, SqlKind.OTHER_FUNCTION,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION,
        framework.getCatalogReader().nameMatcher(), true);
    List<SqlOperator> operators = new ArrayList<>();
    sqlOperatorIterator.forEachRemaining(operators::add);
    return operators;
  }

  private List<String> constructArgNames(List<ArgCombination> args) {
    return args.stream()
        .map(f -> VARIABLE_PREFIX.getCanonical() + f.getDefinition().getName()) //to variable name
        .collect(Collectors.toList());
  }

  private List<RelDataType> constructArgTypes(List<ArgCombination> args, TypeFactory typeFactory) {
    return args.stream()
        .map(i -> graphqlToRelDataType(i.getDefinition().getType(), typeFactory))
        .collect(Collectors.toList());
  }

  public RelDataType graphqlToRelDataType(Type type, RelDataTypeFactory typeFactory) {
    if (type instanceof NonNullType) {
      NonNullType nonNullType = (NonNullType) type;
      return typeFactory.createTypeWithNullability(
          graphqlToRelDataType(nonNullType.getType(), typeFactory), false);
    } else if (type instanceof ListType) {
      ListType listType = (ListType) type;
      return typeFactory.createArrayType(
          graphqlToRelDataType(listType.getType(), typeFactory), -1);
    }
    TypeName typeName = (TypeName) type;
    return framework.getQueryPlanner().parseDatatype(typeName.getName());
  }
}
