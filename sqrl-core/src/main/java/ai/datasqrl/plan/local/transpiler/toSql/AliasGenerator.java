package ai.datasqrl.plan.local.transpiler.toSql;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Table;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class AliasGenerator {

  public static AtomicInteger cur = new AtomicInteger(0);

  public Name nextTableAliasName() {
    return Name.system("__t" + cur.getAndIncrement());
  }

}
