package ai.dataeng.sqml;

import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.util.NamedIdentifier;
import ai.dataeng.sqml.config.util.StringNamedId;
import ai.dataeng.sqml.execution.StreamEngine;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

@Getter
public class ScriptDeployment implements Serializable {

    private final UUID uuid;
    private final Instant submissionTime;
    private final ScriptBundle bundle;

    private String executionId;
    private boolean archived = false;

    public ScriptDeployment(@NonNull ScriptBundle scriptBundle) {
        this(UUID.randomUUID(), Instant.now(), scriptBundle);
    }

    public static ScriptDeployment of(ScriptBundle scriptBundle) {
        return new ScriptDeployment(scriptBundle);
    }

    ScriptDeployment(@NonNull UUID uuid, @NonNull Instant submissionTime, @NonNull  ScriptBundle scriptBundle) {
        this.uuid = uuid;
        this.submissionTime = submissionTime;
        this.bundle = scriptBundle;
    }

    public boolean isActive() {
        return !isArchived();
    }

    public NamedIdentifier getId() {
        StringBuilder s = new StringBuilder();
        s.append(bundle.getName().getDisplay()).append("@").append(bundle.getVersion().getId())
                .append("-").append(uuid.toString());
        return StringNamedId.of(s.toString());
    }

    public Result getStatusResult(StreamEngine streamEngine, Optional<CompilationResult> compileResult) {
        Status status = Status.submitted;
        if (executionId!=null) {
            Optional<? extends StreamEngine.Job> job = streamEngine.getJob(executionId);
            if (job.isEmpty()) status = Status.stopped;
            else {
                switch (job.get().getStatus()) {
                    case FAILED:
                        status = Status.failed;
                        break;
                    case RUNNING:
                        status = Status.running;
                        break;
                    case STOPPED:
                        status = Status.stopped;
                        break;
                }
            }
        }
        Result.ResultBuilder builder = Result.builder()
                .id(getId().getId())
                .name(bundle.getName().getDisplay())
                .version(bundle.getVersion().getId())
                .submissionTime(submissionTime)
                .executionId(executionId)
                .status(status);
        if (compileResult.isPresent()) builder.compilation(compileResult.get());
        return builder.build();
    }

    public void archive() {
        this.archived = true;
    }

    public void setExecutionId(@NonNull String executionId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(executionId));
        this.executionId = executionId;
    }

    public static class Group implements Iterable<ScriptDeployment> {

        private final List<ScriptDeployment> submissions;

        public Group(List<ScriptBundle> bundleGroup) {
            UUID uid = UUID.randomUUID();
            Instant timeNow = Instant.now();
            submissions = bundleGroup.stream().map(sb -> new ScriptDeployment(uid, timeNow, sb))
                    .collect(Collectors.toList());
        }

        public void setExecutionId(@NonNull String executionId) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(executionId));
            submissions.stream().forEach(s -> s.setExecutionId(executionId));
        }

        @Override
        public Iterator<ScriptDeployment> iterator() {
            return submissions.iterator();
        }
    }


    @Value
    @Builder
    public static class Result {

        private final String id;
        private final String name;
        private final String version;
        private final String executionId;
        private final Instant submissionTime;
        private final Status status;
        private final CompilationResult compilation;

    }

    public enum Status {
        submitted, running, deployed, stopped, failed;
    }


}
