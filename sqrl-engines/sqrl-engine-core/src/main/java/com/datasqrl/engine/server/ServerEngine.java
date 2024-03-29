/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.server;

import com.datasqrl.engine.ExecutionEngine;

/**
 * The server engine is a combination of the server core (the graphql engine) and the
 * servlet that is running it. Some servlets may not support things like Java, reflection,
 * code generation executors, etc.
 */
public interface ServerEngine extends ExecutionEngine {

}
