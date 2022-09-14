package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;

@Getter
public class ImportedSourceTable extends AbstractRelationalTable {

    private final SourceTableImport sourceTableImport;
    private RelDataType baseRowType;
    private int timestampIndex = -1;
    private AddedColumn.Simple timestampColumn = null;

    public ImportedSourceTable(@NonNull Name nameId, RelDataType baseRowType, SourceTableImport sourceTableImport) {
        super(nameId);
        this.baseRowType = baseRowType;
        this.sourceTableImport = sourceTableImport;
    }

    @Override
    public RelDataType getRowType() {
        return baseRowType;
    }

    public void setTimestampIndex(int index) {
        Preconditions.checkArgument(index>=0 && index<baseRowType.getFieldCount());
        Preconditions.checkArgument(timestampColumn==null);
        this.timestampIndex = index;
    }

    public void setTimestampColumn(@NonNull AddedColumn.Simple timestampCol, @NonNull RelDataTypeFactory factory) {
        Preconditions.checkArgument(timestampIndex<0);
        this.timestampColumn = timestampCol;
        //Update data type
        this.baseRowType = timestampCol.appendTo(baseRowType,factory);
    }

    @Override
    public List<String> getPrimaryKeyNames() {
        return List.of(ReservedName.UUID.getCanonical());
    }
}
