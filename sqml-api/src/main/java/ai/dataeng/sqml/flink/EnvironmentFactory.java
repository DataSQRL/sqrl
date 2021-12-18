package ai.dataeng.sqml.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

public interface EnvironmentFactory {

    public StreamExecutionEnvironment create();

  TableEnvironment createTableApi();
}
