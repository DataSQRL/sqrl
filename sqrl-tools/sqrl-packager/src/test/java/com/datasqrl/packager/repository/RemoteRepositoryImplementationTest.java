package com.datasqrl.packager.repository;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class RemoteRepositoryImplementationTest {

  @Test
  @Disabled
  void retrieveDependency() {
    RemoteRepositoryImplementation remoteRepositoryImplementation =
        new RemoteRepositoryImplementation();
    JsonNode dev = remoteRepositoryImplementation.getDependencyInfo(
        "datasqrl.profiles.flink-1-16",
        "0.0.1", "dev");

    assertEquals("datasqrl.profiles.flink-1-16", dev.get("name").textValue());
  }
}