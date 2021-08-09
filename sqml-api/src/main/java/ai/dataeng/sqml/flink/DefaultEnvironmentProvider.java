package ai.dataeng.sqml.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DefaultEnvironmentProvider implements EnvironmentProvider {
    @Override
    public StreamExecutionEnvironment get() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }
}
