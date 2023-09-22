package com.datasqrl.function;

import com.datasqrl.function.IndexType;
import com.datasqrl.function.SqrlFunction;
import java.util.EnumSet;
import java.util.function.Predicate;

public interface IndexableFunction extends SqrlFunction {

  OperandSelector getOperandSelector();

  double estimateSelectivity();

  EnumSet<IndexType> getSupportedIndexes();


  interface OperandSelector {

    boolean isSelectableColumn(int columnIndex);

    int maxNumberOfColumns();

  }

}
