package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;

public interface StreamEngine {

    public SourceTableMonitor getSourceTableMonitor();

}
