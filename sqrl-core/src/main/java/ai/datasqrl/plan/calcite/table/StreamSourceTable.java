package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.builder.UniversalTableBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

@Getter
public class StreamSourceTable extends SourceTable {

    @Setter
    private RelNode baseRelation;
    private final RelDataType streamRowType;
    private final UniversalTableBuilder streamSchema;
    private final StateChangeType stateChangeType;

    public StreamSourceTable(@NonNull Name nameId, RelNode baseRelation, RelDataType streamRowType,
                             UniversalTableBuilder streamSchema, StateChangeType stateChangeType) {
        super(nameId);
        this.baseRelation = baseRelation;
        this.streamRowType = streamRowType;
        this.streamSchema = streamSchema;
        this.stateChangeType = stateChangeType;
    }

    @Override
    public RelDataType getRowType() {
        return streamRowType;
    }

    @Override
    public List<String> getPrimaryKeyNames() {
        return List.of(ReservedName.UUID.getCanonical());
    }
}
