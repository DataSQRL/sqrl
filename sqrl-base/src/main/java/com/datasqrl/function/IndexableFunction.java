package com.datasqrl.function;

import com.datasqrl.function.IndexType;
import com.datasqrl.function.SqrlFunction;
import java.util.EnumSet;
import java.util.function.Predicate;

public interface IndexableFunction extends SqrlFunction {

  Predicate<Integer> getOperandSelector();

  double estimateSelectivity();

  EnumSet<IndexType> getSupportedIndexes();


}
