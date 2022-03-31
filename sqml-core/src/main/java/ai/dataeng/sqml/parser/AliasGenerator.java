package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.Name;
import java.util.concurrent.atomic.AtomicInteger;

public class AliasGenerator {

  public static AtomicInteger cur = new AtomicInteger(0);

  public String nextAlias() {
    return "__f" + cur.getAndIncrement();
  }

  public String nextTableAlias() {
    return "__t" + cur.getAndIncrement();
  }
  public Name nextTableAliasName() {
    return Name.system("__t" + cur.getAndIncrement());
  }

  public String nextAnonymousName() {
    return "__a" + cur.getAndIncrement();
  }
}
