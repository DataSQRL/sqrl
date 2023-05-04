/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.serializer.Deserializer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * The server engine is a combination of the server core (the graphql engine) and the
 * servlet that is running it. Some servlets may not support things like Java, reflection,
 * code generation executors, etc.
 */
public interface ServerEngine extends ExecutionEngine {

  EnginePhysicalPlan readPlanFrom(Path directory, String stageName, Deserializer deserializer) throws IOException;

}
