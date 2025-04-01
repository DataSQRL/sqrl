package com.datasqrl.vector;

import static com.datasqrl.vector.VectorFunctions.VEC_TO_DOUBLE;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.table.functions.ScalarFunction;

import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

/**
 * Computes the euclidean distance between two vectors
 */
@AutoService(StandardLibraryFunction.class)
public class EuclideanDistance extends ScalarFunction implements StandardLibraryFunction {

  public double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    // Create RealVectors from the input arrays
    RealVector vA = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorA), false);
    RealVector vB = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorB), false);
    return vA.getDistance(vB);
  }
}
