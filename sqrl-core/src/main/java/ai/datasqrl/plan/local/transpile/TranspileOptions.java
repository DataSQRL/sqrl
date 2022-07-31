package ai.datasqrl.plan.local.transpile;

import lombok.Builder;
import lombok.Builder.Default;

@Builder
public class TranspileOptions {
  @Default
  public boolean orderToOrdinals = true;
}
