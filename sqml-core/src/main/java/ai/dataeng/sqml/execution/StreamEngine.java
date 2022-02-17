package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.config.provider.DatasetRegistryPersistenceProvider;
import ai.dataeng.sqml.config.provider.MetadataStoreProvider;
import ai.dataeng.sqml.execution.sql.DatabaseSink;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import ai.dataeng.sqml.planner.optimize.LogicalPlanOptimizer;
import ai.dataeng.sqml.planner.optimize.MaterializeSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Closeable;
import java.util.Map;

public interface StreamEngine extends Closeable {

    Job getJob(String id);

    interface Job {

        String getId();

        void execute();

        void cancel();

        Status getStatus();

        enum Status { PREPARING, RUNNING, STOPPED, FAILED }

    }

    interface Generator {

        public Job generateStream(String scriptName, LogicalPlanOptimizer.Result logical, Map<MaterializeSource, DatabaseSink> sinkMapper);

    }

    interface SourceMonitor {

        public Job monitorTable(SourceTable table);

    }
}
