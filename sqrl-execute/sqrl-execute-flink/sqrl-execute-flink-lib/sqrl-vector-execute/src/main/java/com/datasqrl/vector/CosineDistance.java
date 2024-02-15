package com.datasqrl.vector;

public class CosineDistance extends CosineSimilarity {

  public double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    return 1 - super.eval(vectorA, vectorB);
  }

}