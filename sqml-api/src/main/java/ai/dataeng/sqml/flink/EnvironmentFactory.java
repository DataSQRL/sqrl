package ai.dataeng.sqml.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface EnvironmentFactory {

    public StreamExecutionEnvironment create();

}
