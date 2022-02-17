package ai.dataeng.sqml.io.sources.dataset;

/**
 * Call-back interface to inform the {@link ai.dataeng.sqml.Environment} to start monitoring a new {@link SourceTable}
 * to compile statistics on the data.
 *
 */
public interface SourceTableMonitor {

    /**
     *
     * @param table
     * @return id that uniquely identifies the monitoring job
     */
    String startTableMonitoring(SourceTable table);

    /**
     *
     * @param id of the monitoring job
     */
    void stopTableMonitoring(String id);

}
