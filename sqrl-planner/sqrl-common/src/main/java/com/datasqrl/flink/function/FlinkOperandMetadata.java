package com.datasqrl.flink.function;

import com.datasqrl.calcite.type.FlinkVectorType;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.calcite.type.VectorType;
import com.datasqrl.flink.FlinkConverter;
import java.util.stream.IntStream;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.core.Aggregate.AggCallBinding;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidator.Config;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqrlSqlValidator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.inference.ArgumentCountRange;
import org.apache.flink.table.types.DataType;
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

  public static SqlCallBinding adaptCallBinding(SqlCallBinding sqlCallBinding, FlinkTypeFactory flinkTypeFactory) {
    CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
        CalciteSchema.createRootSchema(false), List.of(), flinkTypeFactory, null);
    SqlValidator validator = sqlCallBinding.getValidator();
    return new SqlCallBinding(new SqrlSqlValidator(SqlStdOperatorTable.instance(),
        calciteCatalogReader, new TypeFactory(), Config.DEFAULT
    ) {
      @Override
      public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        RelDataType validatedNodeType = validator.deriveType(scope,expr);
        if (validatedNodeType instanceof VectorType) {
          FlinkTypeFactory flinkTypeFactory = new FlinkTypeFactory(getClass().getClassLoader(),
              FlinkTypeSystem.INSTANCE);
          DataType dataType = DataTypes.of(FlinkVectorType.class).toDataType(
              FlinkConverter.catalogManager.getDataTypeFactory());

          RelDataType flinkType = flinkTypeFactory
              .createFieldTypeFromLogicalType(dataType.getLogicalType());

          return flinkType;
        }

        return validator.deriveType(scope, expr);
      }
    }, sqlCallBinding.getScope(), sqlCallBinding.getCall());
  }

  public static AggCallBinding adaptCallBinding(AggCallBinding sqlCallBinding, FlinkTypeFactory flinkTypeFactory,
      TypeFactory typeFactory) {

    List<RelDataType> types = IntStream.range(0, sqlCallBinding.getOperandCount())
        .mapToObj(i -> translateToFlinkType(sqlCallBinding.getOperandType(i), flinkTypeFactory))
        .collect(Collectors.toList());

    AggCallBinding aggCallBinding = new AggCallBinding(flinkTypeFactory, (SqlAggFunction) sqlCallBinding.getOperator(),
        types, sqlCallBinding.getGroupCount(), sqlCallBinding.hasFilter());
    return aggCallBinding;
  }

  public static SqlOperatorBinding adaptCallBinding(RexCallBinding sqlCallBinding,
      FlinkTypeFactory flinkTypeFactory, TypeFactory typeFactory) {
    return sqlCallBinding;
  }
  private static RelDataType translateToFlinkType(RelDataType operandType,
      FlinkTypeFactory flinkTypeFactory) {
    if (operandType instanceof VectorType) {
      DataType dataType = DataTypes.of(FlinkVectorType.class).toDataType(
          FlinkConverter.catalogManager.getDataTypeFactory());

      RelDataType flinkType = flinkTypeFactory
          .createFieldTypeFromLogicalType(dataType.getLogicalType());

      return flinkType;
    }
    return null;
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
