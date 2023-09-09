/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.AbstractGraphqlTest;
import com.datasqrl.util.data.Conference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
public class ConferenceTest extends AbstractGraphqlTest {
  CompletableFuture<ExecutionResult> fut;

  @BeforeEach
  void setUp() {
    fut = execute(Conference.INSTANCE);
    events = new ArrayList<>();
  }

  @SneakyThrows
  @Test
  @Disabled
  public void run() {

  }
}
