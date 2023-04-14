package com.datasqrl.engine.stream.monitor;

import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.StreamSourceProvider;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.util.Metric;

public interface DataMonitor extends StreamSourceProvider {

  <M extends Metric<M>> void monitorTable(TableInput tableSource, StreamHolder<M> stream,
      MetricStore.Provider<M> storeProvider);

  Job build();

  interface Job {

    void execute(String name);

    /**
     * If the underlying engine supports async execution, this method should not block when the
     * job is executed. Otherwise this method is identical to {@link #execute(String)}.
     * @param name
     */
    default void executeAsync(String name) {
      execute(name);
    }

    void cancel();

    Status getStatus();

    enum Status {
      PREPARING, RUNNING, COMPLETED, FAILED;

      public boolean hasStopped() {
        return this==COMPLETED || this==FAILED;
      }
    }

  }


}
