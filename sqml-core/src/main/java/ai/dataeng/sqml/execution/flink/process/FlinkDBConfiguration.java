package ai.dataeng.sqml.execution.flink.process;

import javax.annotation.Nullable;
import lombok.Value;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

@Value
public class FlinkDBConfiguration {

    @Nullable
    private final JdbcConnectionOptions jdbcConnectionOptions;

}
