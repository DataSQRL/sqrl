package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.concurrent.atomic.AtomicInteger;

public class SqrlTable2 {
  private static final AtomicInteger versionInc = new AtomicInteger();

  private final NamePath name;
  private final SqrlResolvedSchema schema;

  private final NamePath versionName;
  private final int version;

  public SqrlTable2(NamePath name, SqrlResolvedSchema schema) {
    this.name = name;
    this.schema = schema;
    this.version = versionInc.incrementAndGet();
    this.versionName = name.version(version);
  }

  public NamePath getVersionedName() {
    return versionName;
  }

  public NamePath getName() {
    return name;
  }

}