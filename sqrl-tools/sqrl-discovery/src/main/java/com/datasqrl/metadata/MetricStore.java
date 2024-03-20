package com.datasqrl.metadata;

import com.datasqrl.io.util.Metric;
import java.io.Closeable;
import java.io.Serializable;

public interface MetricStore<M extends Metric<M>> extends Closeable {

  void put(M metric);

  interface Provider<M extends Metric<M>> extends Serializable {

    MetricStore<M> open();

  }

}
