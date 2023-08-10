
package com.datasqrl.calcite.function.builtin;

import com.datasqrl.calcite.function.ITransformation;
import com.datasqrl.calcite.type.MyVectorType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;
import java.util.Random;

public class MyCosineDistance extends ScalarFunction implements ITransformation {

  public double eval(MyVectorType l, MyVectorType r) {
    double[] lhs = l.vector;
    double[] rhs = r.vector;
    if (lhs.length != rhs.length) {
      throw new IllegalArgumentException("Vectors must be of the same length");
    }

    double dotProduct = 0.0;
    double normLhs = 0.0;
    double normRhs = 0.0;
    for (int i = 0; i < lhs.length; i++) {
      dotProduct += lhs[i] * rhs[i];
      normLhs += lhs[i] * lhs[i];
      normRhs += rhs[i] * rhs[i];
    }

    double denominator = Math.sqrt(normLhs) * Math.sqrt(normRhs);

    if (denominator == 0) {
      throw new ArithmeticException("Division by zero while calculating cosine similarity");
    }

    double cosineSimilarity = dotProduct / denominator;
    return 1.0 - cosineSimilarity;
  }

  @Override
  public SqlNode apply(String dialect, SqlOperator op, SqlParserPos pos, List<SqlNode> nodeList) {
//    switch (dialect) {
//      case "POSTGRES":
//      default:
//        return new SqrlUnresolvedFunction("NOW2")
//            .createCall(pos, nodeList);
//    }
    return null;
  }
}