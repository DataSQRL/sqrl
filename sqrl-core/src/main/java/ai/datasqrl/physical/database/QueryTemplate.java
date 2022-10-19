package ai.datasqrl.physical.database;

import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class QueryTemplate {

    final RelNode relNode;
    //TODO: add parameters

}
