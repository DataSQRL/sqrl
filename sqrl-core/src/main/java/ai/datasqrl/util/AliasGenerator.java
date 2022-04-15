package ai.datasqrl.util;

import ai.datasqrl.parse.tree.name.Name;
import java.util.concurrent.atomic.AtomicInteger;

public class AliasGenerator {
  public static AtomicInteger cur = new AtomicInteger(0);

  public Name nextAliasName() {
    return Name.system("__f" + cur.getAndIncrement());
  }
  public Name nextTableAliasName() {
    return Name.system("__t" + cur.getAndIncrement());
  }
}
