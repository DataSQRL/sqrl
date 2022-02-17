package ai.dataeng.sqml.execution.flink.environment;

import ai.dataeng.sqml.execution.StreamEngine;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class FlinkStreamEngineImpl implements FlinkStreamEngine {

    @Override
    public StreamExecutionEnvironment create() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        FlinkUtilities.enableCheckpointing(env);
        return env;
    }

    @Override
    public StreamEngine.Job getJob(String id) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void close() throws IOException {
        //Nothing to do
    }
}
