package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.server.Environment;

/**
 * Call-back interface to inform the {@link Environment} to start monitoring a new {@link SourceTable}
 * to compile statistics on the data.
 *
 * A {@link SourceTableMonitor} implementation keeps track of the status of the monitoring, continues monitoring in case
 * of recoverable failure, and persists the job id to stop monitoring when needed.
 *
 */
public interface SourceTableMonitor {

    /**
     * Monitors the given table. This call is a no-op if this table is already being monitored.
     *
     * @param table the table to monitor
     */
    void startTableMonitoring(SourceTable table);

    /**
     *
     * @param table the table to stop monitoring
     */
    void stopTableMonitoring(SourceTable table);

}
