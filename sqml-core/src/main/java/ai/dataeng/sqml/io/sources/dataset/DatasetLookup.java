package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.tree.name.Name;
import lombok.NonNull;

public interface DatasetLookup {

    public SourceDataset getDataset(@NonNull Name name);

    default SourceDataset getDataset(@NonNull String name) {
        return getDataset(Name.system(name));
    }

}
