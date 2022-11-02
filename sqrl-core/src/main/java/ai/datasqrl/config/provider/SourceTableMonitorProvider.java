package ai.datasqrl.config.provider;

import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.compile.SourceTableMonitor;

public interface SourceTableMonitorProvider {

  SourceTableMonitor create(StreamEngine engine, TableStatisticsStoreProvider.Encapsulated statsStore);

  SourceTableMonitorProvider NO_MONITORING = (e, m) -> {

    return new SourceTableMonitor() {
      @Override
      public void startTableMonitoring(TableSource table) {
        //Do nothing
      }

      @Override
      public void stopTableMonitoring(TableSource table) {
        //Do nothing;
      }
    };
  };

}
