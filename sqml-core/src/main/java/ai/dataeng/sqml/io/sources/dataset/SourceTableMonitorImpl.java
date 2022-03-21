package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.execution.StreamEngine;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SourceTableMonitorImpl implements SourceTableMonitor {

    StreamEngine stream;
    StreamEngine.SourceMonitor monitor;

    @Override
    public String startTableMonitoring(SourceTable table) {
        StreamEngine.Job job = monitor.monitorTable(table);
        job.execute(table.qualifiedName());
        return job.getId();
    }

    @Override
    public void stopTableMonitoring(String id) {
        Optional<? extends StreamEngine.Job> job = stream.getJob(id);
        if (job.isPresent()) job.get().cancel();
    }
}
