package com.datasqrl.calcite.type;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;

@AllArgsConstructor
public class VectorType extends Number {
  @Delegate
  public Double value;


}
