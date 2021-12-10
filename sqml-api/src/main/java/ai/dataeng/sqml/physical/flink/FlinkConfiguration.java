package ai.dataeng.sqml.physical.flink;

import lombok.Value;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import javax.annotation.Nullable;

@Value
public class FlinkConfiguration {

    @Nullable
    private final JdbcConnectionOptions jdbcConnectionOptions;

}
