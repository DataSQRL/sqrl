package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import lombok.NonNull;

public interface DatasetLookup {

    public SourceDataset getDataset(@NonNull Name name);

    default SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

}
