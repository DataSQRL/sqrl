package com.datasqrl.discovery;

import com.datasqrl.config.TableConfig;
import com.datasqrl.metadata.MetadataStoreProvider;
import java.util.Collection;

public interface MonitoringJobFactory {

  MonitoringJobFactory.Job create(Collection<TableConfig> tables, MetadataStoreProvider storeProvider);

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
