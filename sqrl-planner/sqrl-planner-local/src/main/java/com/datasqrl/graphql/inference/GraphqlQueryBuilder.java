package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.SchemaBuilder.ArgCombination;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import graphql.language.FieldDefinition;
import graphql.language.IntValue;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.language.Value;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandMetadataImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class GraphqlQueryBuilder {

  private final AtomicInteger queryCounter = new AtomicInteger();

  SqrlFramework framework;
  APIConnectorManager apiManager;

  public Model.ArgumentSet create(List<ArgCombination> arg, SqrlTableMacro macro, SQRLTable table,
      String parentName, FieldDefinition fieldDefinition) {

    Pair<SqlUserDefinedTableFunction, Boolean> operatorPair = resolveOperator(macro, arg);
    SqlUserDefinedTableFunction operator = operatorPair.getLeft();
    boolean allowPermutation = operatorPair.getRight();

    String nameId = parentName + "." + fieldDefinition.getName() + "-"
        + queryCounter.incrementAndGet();

    QueryBuilderHelper queryBuilderHelper = new QueryBuilderHelper(framework.getQueryPlanner(),
        operator, framework.getQueryPlanner().getRelBuilder(),
        macro.getRowType(), nameId, apiManager);

    if (allowPermutation) {
      for (SqrlFunctionParameter parameter : getInternalParams(operator.getFunction().getParameters())) {
        queryBuilderHelper.addInternalOperand(parameter.getVariableName(), parameter.getRelDataType()); //todo: how to get scope's name?
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
      ImmutableMap<String, ArgCombination> nameToArg = Maps.uniqueIndex(arg, a -> a.getDefinition().getName().toLowerCase());

      // Iterate over the table function to resolve all parameters it needs
      for (FunctionParameter functionParameter : operator.getFunction().getParameters()) {
        SqrlFunctionParameter parameter = (SqrlFunctionParameter) functionParameter;
        if (parameter.isInternal()) {
          queryBuilderHelper.addInternalOperand(parameter.getVariableName(),
              parameter.getRelDataType());
        } else {
          ArgCombination tableArgument = nameToArg.get(parameter.getVariableName().toLowerCase());
          String operand = tableArgument.getDefinition().getName();
          RelDataType operandType = graphqlToRelDataType(tableArgument.getDefinition().getType(), framework.getTypeFactory());

          if (tableArgument.getDefaultValue().isPresent()) {
            Object value = valueToObject(tableArgument.getDefaultValue().get());
            queryBuilderHelper.addLiteralOperand(operand, operandType, value);
          } else {
            queryBuilderHelper.addVariableOperand(operand, operandType);
          }
        }
      }
      queryBuilderHelper.scan(operator);
    }

    queryBuilderHelper.applyExtraFilters();

    Optional<ArgCombination> limit = onlyLimit(arg);
    Optional<ArgCombination> offset = onlyOffset(arg);
    if (limit.isPresent() || offset.isPresent()) {
      ScriptRelationalTable scriptTable = (ScriptRelationalTable) table.getVt();
      int numPrimaryKeys = scriptTable.getNumPrimaryKeys();

      queryBuilderHelper.limitOffset(limit, offset, numPrimaryKeys);
    }

    Model.ArgumentSet argumentSet = queryBuilderHelper.build();
    return argumentSet;
  }

  private Object valueToObject(Value value) {
    return 1;
  }

  private boolean isLimitOrOffset(ArgCombination c) {
    return c.getDefinition().getName().equalsIgnoreCase("limit") ||
        c.getDefinition().getName().equalsIgnoreCase("offset");
  }

  private Pair<SqlUserDefinedTableFunction, Boolean> resolveOperator(SqrlTableMacro macro, List<ArgCombination> arg) {
    // Lookup operator with full arguments. If not found, look for table fnc with no args and allow permutation
    List<ArgCombination> pagingRemoved = removePaging(arg);
    List<SqrlFunctionParameter> internalParams = getInternalParams(macro.getParameters());
    String tableFunctionName = String.join(".", macro.getFullPath().toStringList());

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


  private List<SqrlFunctionParameter> getInternalParams(List<FunctionParameter> params) {
    return params.stream()
        .map(p -> (SqrlFunctionParameter) p)
        .filter(p -> p.isInternal())
        .collect(Collectors.toList());
  }

  private List<SqrlFunctionParameter> getExternalParams(List<FunctionParameter> params) {
    return params.stream()
        .map(p -> (SqrlFunctionParameter) p)
        .filter(p -> !p.isInternal())
        .collect(Collectors.toList());
  }

  private Pair<Argument, RexDynamicParam> createVariable(List<JdbcParameterHandler> params,
      String name,
      RelDataTypeFactory typeFactory) {
    VariableArgument variableArgument = new VariableArgument(name, null);
    params.add(new ArgumentParameter(name));

    RelDataType sqlType = typeFactory.createSqlType(
        SqlTypeName.ANY);

    RexDynamicParam rexDynamicParam = new RexDynamicParam(sqlType, params.size() - 1);
    return Pair.of(variableArgument, rexDynamicParam);
  }

  private Optional<ArgCombination> onlyLimit(List<ArgCombination> arg) {
    return arg.stream()
        .filter(f -> f.getDefinition().getName().equalsIgnoreCase("limit"))
        .findFirst();
  }

  private Optional<ArgCombination> onlyOffset(List<ArgCombination> arg) {
    return arg.stream()
        .filter(f -> f.getDefinition().getName().equalsIgnoreCase("offset"))
        .findFirst();
  }

  private List<ArgCombination> removePaging(List<ArgCombination> arg) {
    return arg.stream()
        .filter(f -> !f.getDefinition().getName().equalsIgnoreCase("limit")
            && !f.getDefinition().getName().equalsIgnoreCase("offset"))
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

  private List<RexNode> argsToRex(List<ArgCombination> args,
      TableFunction function, OperandMetadataImpl metadata, RexBuilder rexBuilder) {
    //Reorder arg list
    List<String> names = metadata.paramNames();
    args.sort(Comparator.comparingInt(a -> names.indexOf(a.getDefinition().getName())));

    List<RexNode> rexNodes = new ArrayList<>();
    int ordinal = 0;
    for (ArgCombination combination : args) {
      RelDataType type = graphqlToRelDataType(combination.getDefinition().getType(),
          rexBuilder.getTypeFactory());
      if (combination.getDefaultValue().isPresent()) {
        rexNodes.add(rexBuilder.makeLiteral(
            getDefaultValue(combination.getDefaultValue().get()), type, false));
      } else {
        rexNodes.add(rexBuilder.makeDynamicParam(type, ordinal++));
      }
    }

    return rexNodes;
  }

  private Object getDefaultValue(Value value) {
    if (value instanceof IntValue) {
      return ((IntValue) value).getValue();
    }

    return null;
  }

  private List<String> constructArgNames(List<ArgCombination> args) {
    return args.stream()
        .map(f -> "@" + f.getDefinition().getName()) //to variable name
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
