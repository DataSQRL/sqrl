package com.datasqrl.functions.flink;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * Bridges a Flink function to calcite
 */
public class BridgingSqlFunction extends SqlFunction {
  private final DataTypeFactory dataTypeFactory;
  private final FlinkTypeFactory typeFactory;
  private final RexFactory rexFactory;
  private final FunctionDefinition definition;
  private final TypeInference typeInference;

  public BridgingSqlFunction(String name, DataTypeFactory dataTypeFactory, FlinkTypeFactory typeFactory,
                             RexFactory rexFactory, SqlKind kind,
                             FunctionDefinition definition, TypeInference typeInference) {
    super(name, kind,
        createSqlReturnTypeInference(name, dataTypeFactory, definition),
        createSqlOperandTypeInference(name, dataTypeFactory, definition),
        createSqlOperandTypeChecker(name, dataTypeFactory, definition),
        createSqlFunctionCategory());

    this.dataTypeFactory = dataTypeFactory;
    this.typeFactory = typeFactory;
    this.rexFactory = rexFactory;
    this.definition = definition;
    this.typeInference = typeInference;
  }

  public static SqlReturnTypeInference createSqlReturnTypeInference(String name, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkSqlReturnTypeInference(dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlOperandTypeInference createSqlOperandTypeInference(String name, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkSqlOperandTypeInference(dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlOperandTypeChecker createSqlOperandTypeChecker(String name, DataTypeFactory dataTypeFactory, FunctionDefinition definition) {
    return new FlinkSqlOperandTypeChecker(dataTypeFactory, definition, definition.getTypeInference(dataTypeFactory));
  }

  public static SqlFunctionCategory createSqlFunctionCategory() {
    return SqlFunctionCategory.USER_DEFINED_FUNCTION;
  }

  @Override
  public List<String> getParamNames() {
    if (typeInference.getNamedArguments().isPresent()) {
      return typeInference.getNamedArguments().get();
    }
    return super.getParamNames();
  }

  @Override
  public boolean isDeterministic() {
    return definition.isDeterministic();
  }
}