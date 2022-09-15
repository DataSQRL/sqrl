package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

@Getter
public class ImportedSourceTable extends AbstractRelationalTable {

    private final SourceTableImport sourceTableImport;
    private RelDataType baseRowType;

    public ImportedSourceTable(@NonNull Name nameId, RelDataType baseRowType, SourceTableImport sourceTableImport) {
        super(nameId);
        this.baseRowType = baseRowType;
        this.sourceTableImport = sourceTableImport;
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
