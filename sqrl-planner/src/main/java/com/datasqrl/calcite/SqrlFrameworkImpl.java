package com.datasqrl.calcite;

import com.datasqrl.DefaultFunctions;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.calcite.jdbc.SqrlSchema;

@Singleton
public class SqrlFrameworkImpl extends SqrlFramework {

  @Inject
  public SqrlFrameworkImpl(SqrlSchema schema) {
    super(SqrlRelMetadataProvider.INSTANCE,
        SqrlHintStrategyTable.getHintStrategyTable(), NameCanonicalizer.SYSTEM,
        schema);
  }
}
