package ai.datasqrl.plan;

import java.util.List;
import lombok.Value;

@Value
public class ImportLocalPlannerResult extends LocalPlannerResult {

  List<ImportTable> importedPaths;

}
