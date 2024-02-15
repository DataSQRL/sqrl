package com.datasqrl.functions.vector;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.IndexType;
import com.datasqrl.function.IndexableFunction;
import com.datasqrl.vector.EuclideanDistance;
import com.google.auto.service.AutoService;
import java.util.EnumSet;

@AutoService(FunctionMetadata.class)
public class EuclideanDistanceMetadata implements IndexableFunction {


  @Override
  public OperandSelector getOperandSelector() {
    return new OperandSelector() {
      @Override
      public boolean isSelectableColumn(int columnIndex) {
        return true;
      }

      @Override
      public int maxNumberOfColumns() {
        return 1;
      }
    };
  }

  @Override
  public double estimateSelectivity() {
    return 0.1;
  }

  @Override
  public EnumSet<IndexType> getSupportedIndexes() {
    return EnumSet.of(IndexType.VEC_EUCLID);
  }

  @Override
  public Class getMetadataClass() {
    return EuclideanDistance.class;
  }
}
