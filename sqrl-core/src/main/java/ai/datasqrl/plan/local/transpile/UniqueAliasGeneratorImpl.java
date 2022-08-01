package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class UniqueAliasGeneratorImpl implements UniqueAliasGenerator {

  private final Map<String, AtomicInteger> seenCounts;

  public UniqueAliasGeneratorImpl() {
    this(new HashMap<>());
  }

  public UniqueAliasGeneratorImpl(Map<String, AtomicInteger> seenCounts) {
    this.seenCounts = seenCounts;
  }

  @Override
  public String generate(TableWithPK table) {
    return generate(table.getNameId().substring(0, 1));
  }

  @Override
  public String generate(String previousAlias) {
    String firstChar = previousAlias.substring(0, 1);
    seenCounts.putIfAbsent(firstChar, new AtomicInteger());

    return firstChar + seenCounts.get(firstChar).incrementAndGet();
  }

  @Override
  public void notifyTableAlias(String alias) {
    seenCounts.putIfAbsent(alias, new AtomicInteger(1));
  }

  @Override
  public String generateFieldName() {
    seenCounts.putIfAbsent("_f_", new AtomicInteger());

    return "_f_" + seenCounts.get("_f_").incrementAndGet();
  }

  @Override
  public String generatePK() {
    seenCounts.putIfAbsent("_pk_", new AtomicInteger());

    return "_pk_" + seenCounts.get("_pk_").incrementAndGet();
  }
}
