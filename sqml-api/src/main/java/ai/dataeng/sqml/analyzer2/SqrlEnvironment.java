package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

@AllArgsConstructor
public class SqrlEnvironment {
  ImportManager importManager;
  TableEnvironment flinkEnv;

  public void addImport(NamePath namePath) {
    importManager.resolve(namePath);
  }

  public SqrlTable addQuery(NamePath namePath, String sql) {
    Table table = flinkEnv.sqlQuery(sql);

    List<Name> pks = StubQueryRewriter.extractPrimaryKey(flinkEnv, sql);

    SqrlTable queryEntity = new SqrlTable(namePath, table, sql, pks);

    return queryEntity;
  }

}
