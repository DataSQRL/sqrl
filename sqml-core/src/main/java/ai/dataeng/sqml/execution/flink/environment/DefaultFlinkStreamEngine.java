package ai.dataeng.sqml.execution.flink.environment;

import ai.dataeng.sqml.execution.flink.ingest.source.DataSourceMonitor;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@AllArgsConstructor
public class DefaultFlinkStreamEngine implements FlinkStreamEngine {

    @Override
    public StreamExecutionEnvironment create() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        FlinkUtilities.enableCheckpointing(env);
        return env;
    }

    @Override
    public SourceTableMonitor getSourceTableMonitor() {
        return new DataSourceMonitor(this);
    }
}
