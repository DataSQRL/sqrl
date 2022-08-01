package ai.datasqrl.plan.calcite.table;

import java.util.List;

public interface TableWithPK {

  String getNameId();

  List<String> getPrimaryKeys();
}