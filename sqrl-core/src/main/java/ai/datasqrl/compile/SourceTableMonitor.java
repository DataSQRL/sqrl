package ai.datasqrl.compile;

import ai.datasqrl.io.sources.dataset.TableSource;


public interface SourceTableMonitor {

  /**
   * Monitors the given table. This call is a no-op if this table is already being monitored.
   *
   * @param table the table to monitor
   */
  void startTableMonitoring(TableSource table);

  /**
   * @param table the table to stop monitoring
   */
  void stopTableMonitoring(TableSource table);

}
