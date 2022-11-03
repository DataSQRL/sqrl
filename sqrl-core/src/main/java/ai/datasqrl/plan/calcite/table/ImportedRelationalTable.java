package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

@Value
public class ImportedRelationalTable extends SourceRelationalTable {

    SourceTable sourceTable;
    RelDataType baseRowType;

    public ImportedRelationalTable(@NonNull Name nameId, RelDataType baseRowType, SourceTable sourceTable) {
        super(nameId);
        this.baseRowType = baseRowType;
        this.sourceTable = sourceTable;
    }

    @Override
    public RelDataType getRowType() {
        return baseRowType;
    }

    @Override
    public List<String> getPrimaryKeyNames() {
        return List.of(ReservedName.UUID.getCanonical());
    }
}
