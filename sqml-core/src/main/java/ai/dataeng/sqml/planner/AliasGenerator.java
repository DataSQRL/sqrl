package ai.dataeng.sqml.planner;

import java.util.concurrent.atomic.AtomicInteger;

public class AliasGenerator {

  public static AtomicInteger cur = new AtomicInteger(0);

  public String nextAlias() {
    return "__x" + cur.getAndIncrement();
  }
}
