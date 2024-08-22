/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.config.EngineFactory.Type;

/**
 * A jackson serializable object
 */
public interface EnginePhysicalPlan {
  Type getType();
}
