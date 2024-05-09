package com.datasqrl.packager.repository;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

class RemoteRepositoryImplementationTest {

  @Test
  void retrieveDependency() {
    RemoteRepositoryImplementation remoteRepositoryImplementation =
        new RemoteRepositoryImplementation();
    JsonNode dev = remoteRepositoryImplementation.getDependencyInfo(
        "henneberger.test-profile.flink.1-16",
        "0.0.1", "dev");

    assertEquals("henneberger.test-profile.flink.1-16", dev.get("name").textValue());
  }
}