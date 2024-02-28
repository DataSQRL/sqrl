package com.datasqrl.vector;

import static com.datasqrl.vector.VectorFunctions.VEC_TO_DOUBLE;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Computes the cosine similarity between two vectors
 */
public class CosineSimilarity extends ScalarFunction {

  public double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    // Create RealVectors from the input arrays
    RealVector vA = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorA), false);
    RealVector vB = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorB), false);

    // Calculate the cosine similarity
    double dotProduct = vA.dotProduct(vB);
    double normalization = vA.getNorm() * vB.getNorm();

    return dotProduct / normalization;
  }
}
