package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import lombok.NonNull;

public interface DatasetLookup {

    public SourceDataset getDataset(@NonNull Name name);

    default SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

    public SourceTableStatistics getTableStatistics(@NonNull SourceTable table);

}
