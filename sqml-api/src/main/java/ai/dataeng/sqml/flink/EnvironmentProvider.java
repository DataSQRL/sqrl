package ai.dataeng.sqml.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface EnvironmentProvider {

    public StreamExecutionEnvironment get();

}
