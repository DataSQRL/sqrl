package com.datasqrl.calcite.type;

public class VectorType {
  public double[] vector;

  public VectorType(double[] vector) {
    this.vector = vector;
  }

  public double[] getVector() {
    return vector;
  }
}
