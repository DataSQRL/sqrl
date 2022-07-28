package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UniqueAliasGeneratorImpl implements UniqueAliasGenerator {
    Map<String, AtomicInteger> seenCounts = new HashMap<>();

    public UniqueAliasGeneratorImpl(Set<String> existing) {
      for (String s : existing) {
        String firstChar = s.substring(0, 1);
        seenCounts.putIfAbsent(firstChar, new AtomicInteger());
      }
    }

    @Override
    public String generate(TableWithPK table) {
      String firstChar = table.getNameId().substring(0, 1);
      seenCounts.putIfAbsent(firstChar, new AtomicInteger());

      return firstChar + seenCounts.get(firstChar).incrementAndGet();
    }

    @Override
    public String generate(String previousAlias) {
      String firstChar = previousAlias.substring(0, 1);
      seenCounts.putIfAbsent(firstChar, new AtomicInteger());

      return firstChar + seenCounts.get(firstChar).incrementAndGet();
    }

    public String generateFieldName() {
      seenCounts.putIfAbsent("s_", new AtomicInteger());

      return "s_" + seenCounts.get("s_").incrementAndGet();
    }

    public String generatePK() {
      seenCounts.putIfAbsent("_pk_", new AtomicInteger());

      return "_pk_" + seenCounts.get("_pk_").incrementAndGet();
    }
  }
