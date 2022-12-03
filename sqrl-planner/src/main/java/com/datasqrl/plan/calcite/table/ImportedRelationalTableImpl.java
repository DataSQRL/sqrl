package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.ReservedName;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

@Value
public class ImportedRelationalTableImpl extends SourceRelationalTableImpl implements ImportedRelationalTable {

    TableSource tableSource;
    RelDataType baseRowType;

    public ImportedRelationalTableImpl(@NonNull Name nameId, RelDataType baseRowType, TableSource tableSource) {
        super(nameId);
        this.baseRowType = baseRowType;
        this.tableSource = tableSource;
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
