package ai.datasqrl.plan.calcite.sqrl.rules;

import ai.datasqrl.plan.calcite.sqrl.table.FieldIndexPath;
import ai.datasqrl.plan.calcite.sqrl.table.QuerySqrlTable;
import lombok.Value;

@Value
public class FieldReference {

    QuerySqrlTable table;
    int uniqueId;
    FieldIndexPath path;

}
