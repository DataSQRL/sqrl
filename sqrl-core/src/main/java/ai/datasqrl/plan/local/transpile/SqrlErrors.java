package ai.datasqrl.plan.local.transpile;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.BaseMessage;
import org.apache.calcite.sql.validate.SqlValidatorException;

public interface SqrlErrors {

  SqrlErrors SQRL_ERRORS = Resources.create(SqrlErrors.class);

  @BaseMessage("Relationship cannot be selected unless inside of inline-agg")
  Resources.ExInst<SqlValidatorException> columnRequiresInlineAgg();

}
