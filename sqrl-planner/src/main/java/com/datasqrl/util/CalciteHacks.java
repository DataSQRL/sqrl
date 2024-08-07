package com.datasqrl.util;

import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;

public class CalciteHacks {

  /**
   * Happens if some flink code is called and the metadata provider gets reset to flink's provider.
   */
  public static void resetToSqrlMetadataProvider() {
    // Reset sqrl metadata provider defaults
    RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(SqrlRelMetadataProvider.INSTANCE));
  }
}
