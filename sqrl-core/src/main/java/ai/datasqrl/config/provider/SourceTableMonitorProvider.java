package ai.datasqrl.config.provider;

import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.dataset.SourceTableMonitor;

public interface SourceTableMonitorProvider {

  SourceTableMonitor create(StreamEngine engine, StreamEngine.SourceMonitor monitor);

  SourceTableMonitorProvider NO_MONITORING = (e, m) -> {

    return new SourceTableMonitor() {
      @Override
      public void startTableMonitoring(SourceTable table) {
        //Do nothing
      }

      @Override
      public void stopTableMonitoring(SourceTable table) {
        //Do nothing;
      }
    };
  };

}
