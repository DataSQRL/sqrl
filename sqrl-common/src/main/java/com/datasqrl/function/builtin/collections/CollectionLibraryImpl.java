package com.datasqrl.function.builtin.collections;

import com.datasqrl.function.SqrlFunction;
import org.apache.flink.table.functions.AggregateFunction;

public class CollectionLibraryImpl {

  public static final COLLECT_AS_SET COLLECT_AS_SET = new COLLECT_AS_SET();


  // mutable accumulator of structured type for the aggregate function
  public static class SetAccumulator {
    public long sum = 0;
    public int count = 0;
  }

  // function that takes (value BIGINT, weight INT), stores intermediate results in a structured
// type of WeightedAvgAccumulator, and returns the weighted average as BIGINT
  public static class COLLECT_AS_SET extends AggregateFunction<Long, SetAccumulator>
    implements SqrlFunction {

    @Override
    public SetAccumulator createAccumulator() {
      return new SetAccumulator();
    }

    @Override
    public Long getValue(SetAccumulator acc) {
      if (acc.count == 0) {
        return null;
      } else {
        return acc.sum / acc.count;
      }
    }

    public void accumulate(SetAccumulator acc, Long iValue, Integer iWeight) {
      acc.sum += iValue * iWeight;
      acc.count += iWeight;
    }

    public void retract(SetAccumulator acc, Long iValue, Integer iWeight) {
      acc.sum -= iValue * iWeight;
      acc.count -= iWeight;
    }

    public void merge(SetAccumulator acc, Iterable<SetAccumulator> it) {
      for (SetAccumulator a : it) {
        acc.count += a.count;
        acc.sum += a.sum;
      }
    }

    public void resetAccumulator(SetAccumulator acc) {
      acc.count = 0;
      acc.sum = 0L;
    }
  }

}
