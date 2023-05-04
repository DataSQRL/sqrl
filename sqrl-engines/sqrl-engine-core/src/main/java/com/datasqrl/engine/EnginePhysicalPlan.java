/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Path;

public interface EnginePhysicalPlan {

  void writeTo(Path deployDir, String stageName, Deserializer serializer) throws IOException;

}
