package com.datasqrl.function;

import com.datasqrl.plan.global.IndexType;
import java.util.EnumSet;

public interface IndexableFunction extends FunctionMetadata {

  OperandSelector getOperandSelector();

  double estimateSelectivity();

  EnumSet<IndexType> getSupportedIndexes();


  interface OperandSelector {

    boolean isSelectableColumn(int columnIndex);

    int maxNumberOfColumns();

  }

}
