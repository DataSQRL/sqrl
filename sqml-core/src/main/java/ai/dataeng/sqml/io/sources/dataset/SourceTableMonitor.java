package ai.dataeng.sqml.io.sources.dataset;

/**
 * Call-back interface to inform the {@link ai.dataeng.sqml.Environment} to start monitoring a new {@link SourceTable}
 * to compile statistics on the data.
 *
 */
@FunctionalInterface
public interface SourceTableMonitor {

    void startTableMonitoring(SourceTable table);

}
