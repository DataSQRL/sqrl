package com.datasqrl.function;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.function.RuleTransform;
import com.datasqrl.calcite.type.VectorType;
import com.datasqrl.canonicalizer.NamePath;
import com.google.auto.service.AutoService;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(StdLibrary.class)
public class StdVectorLibraryImpl extends AbstractFunctionModule implements StdLibrary {

  public static final NamePath LIB_NAME = NamePath.of("vector");

  public static final List<SqrlFunction> SQRL_FUNCTIONS = List.of(
      new CosineSimilarity(),
      new CosineDistance(),
      new EuclideanDistance(),
      new WeightedAvgExample()
  );

  public StdVectorLibraryImpl() {
    super(SQRL_FUNCTIONS.stream()
        .map(NamespaceObjectUtil::createNsObject)
        .collect(Collectors.toList()));
  }

  public static class CosineDistance extends ScalarFunction implements SqrlFunction, RuleTransform {
    public double eval(VectorType vectorA, VectorType vectorB) {
      // Create RealVectors from the input arrays
      RealVector vA = new ArrayRealVector(vectorA.getVector(), false);
      RealVector vB = new ArrayRealVector(vectorB.getVector(), false);

      // Calculate the cosine similarity
      double dotProduct = vA.dotProduct(vB);
      double normalization = vA.getNorm() * vB.getNorm();

      return dotProduct / normalization;
    }

    @Override
    public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
      return List.of(new SimpleCallTransform(operator,
          ((rexBuilder, call) -> rexBuilder
              .makeCall(PgVectorOperatorTable.CosineDistance, call.getOperands()))));
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  public static class CosineSimilarity extends ScalarFunction implements SqrlFunction, RuleTransform {
    public double eval(VectorType vectorA, VectorType vectorB) {
      return 1 - new CosineDistance().eval(vectorA, vectorB);
    }

    @Override
    public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
      return List.of(new SimpleCallTransform(operator,
          ((rexBuilder, call) -> rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
              rexBuilder.makeExactLiteral(BigDecimal.ONE),
              rexBuilder.makeCall(PgVectorOperatorTable.CosineDistance, call.getOperands())))));
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  public static class EuclideanDistance extends ScalarFunction implements SqrlFunction, RuleTransform {
    public double eval(VectorType vectorA, VectorType vectorB) {
      // Create RealVectors from the input arrays
      RealVector vA = new ArrayRealVector(vectorA.getVector(), false);
      RealVector vB = new ArrayRealVector(vectorB.getVector(), false);

      return vA.getDistance(vB);
    }

    @Override
    public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
      return List.of(new SimpleCallTransform(operator,
          ((rexBuilder, call) ->
              rexBuilder.makeCall(PgVectorOperatorTable.EuclideanDistance, call.getOperands()))));
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  @Override
  public NamePath getPath() {
    return LIB_NAME;
  }

  public static class PgVectorOperatorTable {
    public static final SqlBinaryOperator CosineDistance = new SqlBinaryOperator("<=>",
        SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.DOUBLE),
        null, null);
    public static final SqlBinaryOperator EuclideanDistance = new SqlBinaryOperator("<->",
        SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.DOUBLE),
        null, null);

    private static SqlUnresolvedFunction op(String name) {
      return new SqlUnresolvedFunction(new SqlIdentifier(name, SqlParserPos.ZERO),
          null, null, null,
          null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
  }


  //Example aggregate function
  public static class WeightedAvgExample extends AggregateFunction<Integer, WeightedAvgAccumulator> implements SqrlFunction {

    @Override
    public WeightedAvgAccumulator createAccumulator() {
      return new WeightedAvgAccumulator();
    }

    @Override
    public Integer getValue(WeightedAvgAccumulator acc) {
      if (acc.count == 0) {
        return null;
      } else {
        return acc.sum / acc.count;
      }
    }

    public void accumulate(WeightedAvgAccumulator acc, Integer iValue, Integer iWeight) {
      acc.sum += iValue * iWeight;
      acc.count += iWeight;
    }

    public void retract(WeightedAvgAccumulator acc, Integer iValue, Integer iWeight) {
      acc.sum -= iValue * iWeight;
      acc.count -= iWeight;
    }

    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
      for (WeightedAvgAccumulator a : it) {
        acc.count += a.count;
        acc.sum += a.sum;
      }
    }

    public void resetAccumulator(WeightedAvgAccumulator acc) {
      acc.count = 0;
      acc.sum = 0;
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  // mutable accumulator of structured type for the aggregate function
  public static class WeightedAvgAccumulator {
    public int sum = 0;
    public int count = 0;
  }
}
