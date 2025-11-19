/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.function.translation.postgres.vector;

import com.datasqrl.flinkrunner.stdlib.vector.euclidean_distance;
import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.IndexableFunction;
import com.datasqrl.plan.global.IndexType;
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
    return EnumSet.of(IndexType.VECTOR_EUCLID);
  }

  @Override
  public Class getMetadataClass() {
    return euclidean_distance.class;
  }
}
