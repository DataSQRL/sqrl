package ai.datasqrl.schema;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class UnionSQRLTable extends SQRLTable {

  private final List<SQRLTable> tables = new ArrayList<>();

}
