package com.datasqrl.plan.calcite.table;

import com.datasqrl.parse.tree.name.Name;
import com.datasqrl.parse.tree.name.ReservedName;
import lombok.NonNull;

import java.util.List;

public abstract class SourceRelationalTableImpl extends AbstractRelationalTable implements SourceRelationalTable {

    protected SourceRelationalTableImpl(@NonNull Name nameId) {
        super(nameId);
    }

    @Override
    public List<String> getPrimaryKeyNames() {
        return List.of(ReservedName.UUID.getCanonical());
    }

}
