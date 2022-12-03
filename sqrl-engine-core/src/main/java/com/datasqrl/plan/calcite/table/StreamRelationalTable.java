package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.schema.UniversalTableBuilder;
import org.apache.calcite.rel.RelNode;

public interface StreamRelationalTable extends SourceRelationalTable {

    UniversalTableBuilder getStreamSchema();

    RelNode getBaseRelation();

    StateChangeType getStateChangeType();

}
