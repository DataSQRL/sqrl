package ai.datasqrl.plan.local.transpile;

import lombok.Value;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlQualified;

@Value
public class InlineAggExtractResult {

  SqlIdentifier newIdentifier;
  SqlNode query;
  SqlNode condition;
  SqlNode selectListOrigin;
  SqlQualified qualifiedOrigin;
}