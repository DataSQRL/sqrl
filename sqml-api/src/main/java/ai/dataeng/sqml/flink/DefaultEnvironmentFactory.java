package ai.dataeng.sqml.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DefaultEnvironmentFactory implements EnvironmentFactory {

    @Override
    public StreamExecutionEnvironment create() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        FlinkUtilities.enableCheckpointing(env);
        return env;
    }
}
