package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;

import java.util.concurrent.atomic.AtomicInteger;

public interface SourceTableMonitorProvider {

    SourceTableMonitor create(StreamEngine engine, StreamEngine.SourceMonitor monitor);

    static SourceTableMonitorProvider NO_MONITORING = (e,m) -> {

        final AtomicInteger counter = new AtomicInteger(0);

        return new SourceTableMonitor() {
            @Override
            public String startTableMonitoring(SourceTable table) {
                return String.valueOf(counter.incrementAndGet());
            }

            @Override
            public void stopTableMonitoring(String id) {
                //Do nothing;
            }
        };
    };

}
