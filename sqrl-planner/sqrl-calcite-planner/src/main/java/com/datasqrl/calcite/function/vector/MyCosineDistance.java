
package com.datasqrl.calcite.function.vector;

import com.datasqrl.calcite.type.MyVectorType;
import org.apache.flink.table.functions.ScalarFunction;

public class MyCosineDistance extends ScalarFunction {

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
}