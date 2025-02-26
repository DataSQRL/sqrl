/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import java.io.Serializable;

public interface Accumulator<V, A, C> extends Serializable {

  void add(V value, C context);

  void merge(A accumulator);
}
