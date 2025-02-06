package com.datasqrl.vector;

//import com.google.common.base.Preconditions;

// mutable accumulator of structured type for the aggregate function
public class CenterAccumulator {

  public double[] sum = null;
  public int count = 0;

  public synchronized void add(double[] values) {
    if (count == 0) {
      sum = values.clone();
      count = 1;
    } else {
//      Preconditions.checkArgument(values.length == sum.length);
      for (var i = 0; i < values.length; i++) {
        sum[i] += values[i];
      }
      count++;
    }
  }

  public synchronized void addAll(CenterAccumulator other) {
    if (other.count == 0) {
      return;
    }
    if (this.count == 0) {
      this.sum = new double[other.sum.length];
    }
//    Preconditions.checkArgument(this.sum.length == other.sum.length);
    for (var i = 0; i < other.sum.length; i++) {
      this.sum[i] += other.sum[i];
    }
    this.count += other.count;
  }

  public double[] get() {
//    Preconditions.checkArgument(count > 0);
    var result = new double[sum.length];
    for (var i = 0; i < sum.length; i++) {
      result[i] = sum[i] / count;
    }
    return result;
  }

  public synchronized void substract(double[] values) {
//    Preconditions.checkArgument(values.length == sum.length);
    for (var i = 0; i < values.length; i++) {
      sum[i] -= values[i];
    }
    count--;
  }
}
