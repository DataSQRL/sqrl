package com.datasqrl.calcite.type;

public class VectorType {
  private double[] vector;

  public VectorType(double[] vector) {
    this.vector = vector;
  }

  public double[] getVector() {
    return vector;
  }
}
