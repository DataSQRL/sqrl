package ai.dataeng.sqml.execution.flink.environment;

import ai.dataeng.sqml.execution.StreamEngine;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalFlinkStreamEngineImpl implements FlinkStreamEngine {

    private final ConcurrentHashMap<String,LocalJob> jobs = new ConcurrentHashMap<>();

    @Override
    public StreamExecutionEnvironment createStream() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        FlinkUtilities.enableCheckpointing(env);
        return env;
    }

    @Override
    public FlinkJob createStreamJob(StreamExecutionEnvironment execEnv, JobType type) {
        return new LocalJob(execEnv,type);
    }

    @Override
    public Optional<StreamEngine.Job> getJob(String id) {
        StreamEngine.Job job = jobs.get(id);
        return Optional.ofNullable(job);
    }

    @Override
    public void close() throws IOException {
        jobs.clear();
    }

    class LocalJob extends FlinkJob {

        protected LocalJob(StreamExecutionEnvironment execEnv, JobType type) {
            super(execEnv, type);
        }

        @Override
        public void execute(String name) {
            super.execute(name);
            if (status!=Status.FAILED) {
                jobs.put(getId(),this);
            }
        }
    }
}
