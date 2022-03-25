package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.execution.StreamEngine;
import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 * TODO: restart monitoring jobs on minor failures, make sure this is resilient
 */
@AllArgsConstructor
public class SourceTableMonitorImpl implements SourceTableMonitor {

    StreamEngine stream;
    StreamEngine.SourceMonitor monitor;

    @Override
    public void startTableMonitoring(SourceTable table) {
        //TODO: check if this table is already being monitored
        StreamEngine.Job job = monitor.monitorTable(table);
        job.execute(table.qualifiedName());
//        return job.getId();
    }

    @Override
    public void stopTableMonitoring(SourceTable table) {
        //TODO: Look up job by qualified table name and stop if it exists
        throw new UnsupportedOperationException("not yet implemented");
//        Optional<? extends StreamEngine.Job> job = stream.getJob(id);
//        if (job.isPresent()) job.get().cancel();
    }
}
