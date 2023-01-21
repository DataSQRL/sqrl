package com.datasqrl.engine.stream.monitor;

import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.StreamSourceProvider;
import com.datasqrl.io.tables.TableInput;

public interface DataMonitor extends StreamSourceProvider {

  <M extends Metric<M>> void monitorTable(TableInput tableSource, StreamHolder<M> stream,
      MetricStore.Provider<M> storeProvider);

  Job build();

  interface Metric<M extends Metric> {

    void merge(M other);

  }

  interface Job {

    void execute(String name);

    void cancel();

    Status getStatus();

    enum Status {PREPARING, RUNNING, COMPLETED, FAILED}

  }


}
