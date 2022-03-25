package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import java.util.concurrent.atomic.AtomicInteger;

public interface SourceTableMonitorProvider {

    SourceTableMonitor create(StreamEngine engine, StreamEngine.SourceMonitor monitor);

    static SourceTableMonitorProvider NO_MONITORING = (e,m) -> {

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
