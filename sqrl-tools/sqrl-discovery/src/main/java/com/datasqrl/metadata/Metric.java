package com.datasqrl.metadata;

public interface Metric<M extends Metric> {

  void merge(M other);

}
