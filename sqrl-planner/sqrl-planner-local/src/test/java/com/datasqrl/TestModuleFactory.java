package com.datasqrl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.local.analyze.FuzzingRetailSqrlModule;
import com.datasqrl.plan.local.analyze.RetailSqrlModule;
import com.datasqrl.plan.table.CalciteTableFactory;
import java.util.HashMap;
import java.util.Map;

public class TestModuleFactory {

  public static Map<NamePath, SqrlModule> createRetail(CalciteTableFactory tableFactory) {
    RetailSqrlModule retailSqrlModule = new RetailSqrlModule();
    retailSqrlModule.init(tableFactory);
    return Map.of(NamePath.of("ecommerce-data"),
        retailSqrlModule);
  }

  public static Map<NamePath, SqrlModule> createFuzz(CalciteTableFactory tableFactory) {
    FuzzingRetailSqrlModule fuzzingRetailSqrlModule = new FuzzingRetailSqrlModule();
    fuzzingRetailSqrlModule.init(tableFactory);
    return Map.of(NamePath.of("ecommerce-data-large"), fuzzingRetailSqrlModule);
  }

  public static Map<NamePath, SqrlModule> merge(Map<NamePath, SqrlModule>... modules) {
    Map<NamePath, SqrlModule> map = new HashMap<>();
    for (Map<NamePath, SqrlModule> m : modules) {
      map.putAll(m);
    }
    return map;
  }
}
