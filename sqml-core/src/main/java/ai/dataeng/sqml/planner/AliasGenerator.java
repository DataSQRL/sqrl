package ai.dataeng.sqml.planner;

import java.util.concurrent.atomic.AtomicInteger;

public class AliasGenerator {

  public static AtomicInteger cur = new AtomicInteger(0);

  public String nextAlias() {
    return "__f" + cur.getAndIncrement();
  }

  public String nextTableAlias() {
    return "__t" + cur.getAndIncrement();
  }

  public String nextAnonymousName() {
    return "__a" + cur.getAndIncrement();
  }
}
