package com.datasqrl.vector;

import static com.datasqrl.vector.VectorFunctions.VEC_TO_DOUBLE;
import static com.datasqrl.vector.VectorFunctions.convert;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregates vectors by computing the centroid, i.e. summing up all vectors and dividing the
 * resulting vector by the number of vectors.
 */
public class Center extends AggregateFunction<FlinkVectorType, CenterAccumulator> {

  @Override
  public CenterAccumulator createAccumulator() {
    return new CenterAccumulator();
  }

  @Override
  public FlinkVectorType getValue(CenterAccumulator acc) {
    if (acc.count == 0) {
      return null;
    } else {
      return convert(acc.get());
    }
  }

  public void accumulate(CenterAccumulator acc, FlinkVectorType vector) {
    acc.add(VEC_TO_DOUBLE.eval(vector));
  }

  public void retract(CenterAccumulator acc, FlinkVectorType vector) {
    acc.substract(VEC_TO_DOUBLE.eval(vector));
  }

  public void merge(CenterAccumulator acc, Iterable<CenterAccumulator> iter) {
    for (CenterAccumulator a : iter) {
      acc.addAll(a);
    }
  }

  public void resetAccumulator(CenterAccumulator acc) {
    acc.count = 0;
    acc.sum = null;
  }
}
