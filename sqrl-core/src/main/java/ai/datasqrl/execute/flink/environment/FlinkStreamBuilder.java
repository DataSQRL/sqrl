package ai.datasqrl.execute.flink.environment;

import lombok.Getter;
import lombok.NonNull;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

@Getter
public class FlinkStreamBuilder implements FlinkStreamEngine.Builder {

    private final FlinkStreamEngine engine;
    private final StreamExecutionEnvironment environment;
    private final UUID uuid;
    private FlinkStreamEngine.JobType jobType;

    public FlinkStreamBuilder(FlinkStreamEngine engine, StreamExecutionEnvironment environment) {
        this.engine = engine;
        this.environment = environment;
        this.uuid = UUID.randomUUID();
    }

    @Override
    public void setJobType(@NonNull FlinkStreamEngine.JobType jobType) {
        this.jobType = jobType;
    }

    @Override
    public FlinkStreamEngine.FlinkJob build() {
        return engine.createStreamJob(environment,jobType);
    }

}
