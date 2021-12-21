package ai.dataeng.sqml.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DefaultEnvironmentFactory implements EnvironmentFactory {

    @Override
    public StreamExecutionEnvironment create() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        FlinkUtilities.enableCheckpointing(env);
        return env;
    }

    @Override
    public TableEnvironment createTableApi() {
        final EnvironmentSettings settings =
            EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);
        return env;
    }
}
