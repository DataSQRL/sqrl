package com.datasqrl.io.util;

public interface Metric<M extends Metric> {

  void merge(M other);

}
