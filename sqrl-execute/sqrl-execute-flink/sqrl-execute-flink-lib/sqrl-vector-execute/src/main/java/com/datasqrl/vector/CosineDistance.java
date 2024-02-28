package com.datasqrl.vector;

/**
 * Computes the cosine distance between two vectors
 */
public class CosineDistance extends CosineSimilarity {

  public double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    return 1 - super.eval(vectorA, vectorB);
  }

}