package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import java.io.Closeable;
import java.util.Optional;
import java.util.UUID;

public interface StreamEngine extends Closeable {

    Builder createStream();

    interface Builder {

        Job build();

    }

    Optional<? extends Job> getJob(String id);

    interface Job {

        String getId();

        void execute(String name);

        void cancel();

        Status getStatus();

        enum Status { PREPARING, RUNNING, STOPPED, FAILED }

    }

    interface Generator {

//        public Job generateStream(LogicalPlanResult logical, Map<MaterializeSource, DatabaseSink> sinkMapper);

    }

    interface SourceMonitor {

        public Job monitorTable(SourceTable table);

    }
}
