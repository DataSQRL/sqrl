package ai.datasqrl.schema.table;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Value;

@Value
public class ColumnReference extends Field {

    final boolean isVisible;

    ColumnReference(Name name, int version, boolean isVisible) {
        super(name, version);
        this.isVisible = isVisible;
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
