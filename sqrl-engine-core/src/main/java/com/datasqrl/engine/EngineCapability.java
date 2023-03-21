/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

public enum EngineCapability {

  DENORMALIZE,
  TEMPORAL_JOIN,
  TO_STREAM,
  TIME_WINDOW_AGGREGATION,
  NOW,
  GLOBAL_SORT,
  MULTI_RANK,
  EXTENDED_FUNCTIONS,
  CUSTOM_FUNCTIONS,
  DATA_MONITORING;

}
