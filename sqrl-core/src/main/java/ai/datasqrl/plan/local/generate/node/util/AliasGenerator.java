package ai.datasqrl.plan.local.generate.node.util;

import ai.datasqrl.parse.tree.name.Name;
import java.util.concurrent.atomic.AtomicInteger;

public class AliasGenerator {

  public static AtomicInteger cur = new AtomicInteger(0);

  public Name nextTableAliasName() {
    return Name.system("__t" + cur.getAndIncrement());
  }

}
