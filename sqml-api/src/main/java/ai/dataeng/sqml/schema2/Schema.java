package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.name.Name;

import java.util.Map;

public class Schema<F extends Field> {

    Map<Name,RelationType<F>> datasets;

    public RelationType<F> getDataset(Name name) {
        return datasets.get(name);
    }

}
