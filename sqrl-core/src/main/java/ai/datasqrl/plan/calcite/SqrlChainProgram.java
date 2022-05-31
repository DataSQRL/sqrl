package ai.datasqrl.plan.calcite;

import java.util.List;
import lombok.Value;

@Value
public class SqrlChainProgram {
  List<SqrlProgram> programs;
}
