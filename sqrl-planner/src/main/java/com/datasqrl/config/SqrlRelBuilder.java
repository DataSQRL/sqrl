package com.datasqrl.config;

import com.datasqrl.calcite.SqrlFramework;
import com.google.inject.Inject;
import lombok.experimental.Delegate;
import org.apache.calcite.tools.RelBuilder;

public class SqrlRelBuilder extends RelBuilder {

  @Inject
  public SqrlRelBuilder(SqrlFramework framework) {
    super(null, framework.getQueryPlanner().getCluster(), framework.getCatalogReader());
  }
}
