package ai.dataeng.sqml.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DefaultEnvironmentProvider implements EnvironmentProvider {
    @Override
    public StreamExecutionEnvironment get() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
