package ai.dataeng.sqml.importer;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.basic.IntegerType;
import java.util.ArrayList;
import java.util.List;

public class DatasetManagerImpl implements DatasetManager {

  @Override
  public void register() {

  }

  @Override
  public Dataset getDataset(Name first) {
    Column field = new Column(
        Name.system("id"), null, 0, IntegerType.INSTANCE,
        0, List.of(), false, false);
    Table table = new Table(0, Name.system("orders"), false);
    table.addField(field);
    List<Table> tables = new ArrayList<>();
    tables.add(table);
    return new Dataset(
        Name.system("ecommerce-data"),
        tables
    );
  }
}
