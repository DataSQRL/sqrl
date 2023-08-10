package com.datasqrl.flink.function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.ArgumentCountRange;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.List;
import java.util.stream.Collectors;

public class FlinkOperandMetadata implements SqlOperandMetadata {
  private final FlinkTypeFactory flinkTypeFactory;
  private final DataTypeFactory dataTypeFactory;
  private final FunctionDefinition definition;
  private final TypeInference typeInference;
  private final ArgumentCountRange countRange;
  private final List<Signature> inputArgDefs;

  public FlinkOperandMetadata(FlinkTypeFactory flinkTypeFactory, DataTypeFactory dataTypeFactory, FunctionDefinition definition, TypeInference typeInference) {
    this.flinkTypeFactory = flinkTypeFactory;
    this.dataTypeFactory = dataTypeFactory;
    this.definition = definition;
    this.typeInference = typeInference;
    this.countRange =
        new ArgumentCountRange(typeInference.getInputTypeStrategy().getArgumentCount());

    this.inputArgDefs = typeInference.getInputTypeStrategy().getExpectedSignatures(definition);
  }

  @Override
  public List<RelDataType> paramTypes(RelDataTypeFactory relDataTypeFactory) {
    return inputArgDefs.stream()
        .flatMap(s->s.getArguments().stream())
        .map(s->relDataTypeFactory.createSqlType(SqlTypeName.valueOf(s.getType())))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> paramNames() {
    return inputArgDefs.stream()
        .flatMap(s->s.getArguments().stream())
        .map(s->s.getName().orElse(null))
        .collect(Collectors.toList());
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean throwOnFailure) {
    return new FlinkSqlOperandTypeChecker(flinkTypeFactory, dataTypeFactory, definition, typeInference)
        .checkOperandTypes(sqlCallBinding, throwOnFailure);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return this.countRange;
  }

  @Override
  public String getAllowedSignatures(SqlOperator sqlOperator, String s) {
    return null;
  }

  @Override
  public Consistency getConsistency() {
    return Consistency.NONE;
  }

  @Override
  public boolean isOptional(int i) {
    return false;
  }
}
