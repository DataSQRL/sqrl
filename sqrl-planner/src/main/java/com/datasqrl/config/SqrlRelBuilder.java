package com.datasqrl.config;

import org.apache.calcite.tools.RelBuilder;

import com.datasqrl.calcite.SqrlFramework;
import com.google.inject.Inject;

public class SqrlRelBuilder extends RelBuilder {

  @Inject
  public SqrlRelBuilder(SqrlFramework framework) {
    super(null, framework.getQueryPlanner().getCluster(), framework.getCatalogReader());
  }
}
