package com.datasqrl.calcite;

import com.datasqrl.DefaultFunctions;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.google.inject.Singleton;

@Singleton
public class SqrlFrameworkImpl extends SqrlFramework {

  public SqrlFrameworkImpl() {
    super(SqrlRelMetadataProvider.INSTANCE,
        SqrlHintStrategyTable.getHintStrategyTable(), NameCanonicalizer.SYSTEM);

    DefaultFunctions functions = new DefaultFunctions();
    functions.getDefaultFunctions()
        .forEach((key, value) -> getSqrlOperatorTable().addFunction(key, value));
  }
}
