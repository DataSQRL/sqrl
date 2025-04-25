package com.datasqrl.functions.vector;

import java.util.EnumSet;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.IndexType;
import com.datasqrl.function.IndexableFunction;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class TextSearchMetadata implements IndexableFunction {

  @Override
  public OperandSelector getOperandSelector() {
    return new OperandSelector() {
      @Override
      public boolean isSelectableColumn(int columnIndex) {
        return columnIndex > 0;
      }

      @Override
      public int maxNumberOfColumns() {
        return 128;
      }
    };
  }

  @Override
  public double estimateSelectivity() {
    return 0.1;
  }

  @Override
  public EnumSet<IndexType> getSupportedIndexes() {
    return EnumSet.of(IndexType.TEXT);
  }

  @Override
  public Class getMetadataClass() {
    return TextSearchMetadata.class;
  }
}
