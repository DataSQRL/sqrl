/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.sensor;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.AbstractGraphqlTest;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.CompletableFuture;

public abstract class SensorsTest extends AbstractGraphqlTest {
  CompletableFuture<ExecutionResult> fut;

  String alert = "subscription HighTempAlert {\n"
      + "  HighTempAlert(sensorid: 1) {\n"
      + "    sensorid\n"
      + "    temp\n"
      + "  }\n"
      + "}";
  String addReading = "mutation AddReading($sensorId: Int!, $temperature: Float!) {\n"
      + "  AddReading(metric: {sensorid: $sensorId, temperature: $temperature}) {\n"
      + "    _source_time\n"
      + "  }\n"
      + "}";

  JsonObject triggerAlertJson = new JsonObject().put("sensorId", 1).put("temperature", 62.1);
  JsonObject nontriggerAlertJson = new JsonObject().put("sensorId", 2).put("temperature", 62.1);

}
