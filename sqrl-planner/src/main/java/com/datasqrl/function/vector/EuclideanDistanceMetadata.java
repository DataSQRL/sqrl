package com.datasqrl.function.vector;

import java.util.EnumSet;

import com.datasqrl.flinkrunner.functions.vector.euclidean_distance;
import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.plan.global.IndexType;
import com.datasqrl.function.IndexableFunction;
import com.google.auto.service.AutoService;

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
    return EnumSet.of(IndexType.VECTOR_EUCLID);
  }

  @Override
  public Class getMetadataClass() {
    return euclidean_distance.class;
  }
}
