package ai.dataeng.sqml.execution.flink.ingest;

import ai.dataeng.sqml.execution.flink.ingest.source.SourceTable;
import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceDataset;
import lombok.NonNull;

public interface DatasetLookup {

    public SourceDataset getDataset(@NonNull Name name);

    default SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

    public SourceTableStatistics getTableStatistics(@NonNull SourceTable table);

}
