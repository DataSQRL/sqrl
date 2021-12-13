package ai.dataeng.sqml.function;

import ai.dataeng.sqml.function.CountFunction.Accumulator;
import ai.dataeng.sqml.function.definition.BuiltInFunctionDefinitions;
import ai.dataeng.sqml.function.definition.inference.TypeInference;

public class CountFunction extends AggregateFunction<Long, Accumulator> {

  public static class Accumulator {
    public long count = 0L;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator();
  }

  @Override
  public Long getValue(Accumulator accumulator) {
    return accumulator.count;
  }

  @Override
  public void accumulate(Accumulator accumulator, Long... i) {
    if (i[0] != null) {
      accumulator.count += i[0];
    }
  }

  @Override
  public void retract(Accumulator accumulator, Long... i) {
    if (i[0] != null) {
      accumulator.count += i[0];
    }
  }

  @Override
  public void merge(Accumulator accumulator, Iterable<Accumulator> iterable) {
    for (Accumulator acc : iterable){
      accumulator.count += acc.count;
    }
  }

  @Override
  public TypeInference getTypeInference() {
    return BuiltInFunctionDefinitions.COUNT.getTypeInference();
  }
}
