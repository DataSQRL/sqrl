package com.datasqrl.discovery.stats;

public interface Metric<M extends Metric> {

  void merge(M other);
}
