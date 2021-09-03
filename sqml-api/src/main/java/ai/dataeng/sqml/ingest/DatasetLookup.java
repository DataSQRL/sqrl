package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.schema.name.Name;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import lombok.NonNull;

public interface DatasetLookup {

    public SourceDataset getDataset(@NonNull Name name);

    default SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

}
