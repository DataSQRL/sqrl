package com.datasqrl.io.stats;

import java.io.Serializable;

public interface Accumulator<V, A, C> extends Serializable {

  void add(V value, C context);

  void merge(A accumulator);

}
