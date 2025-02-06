package com.datasqrl.packager.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class RemoteRepositoryImplementationTest {

  @Test
  @Disabled
  void retrieveDependency() {
    var remoteRepositoryImplementation =
        new RemoteRepositoryImplementation();
    var dev = remoteRepositoryImplementation.getDependencyInfo(
        "datasqrl.profiles.flink-1-16",
        "0.0.1", "dev");

    assertEquals("datasqrl.profiles.flink-1-16", dev.get("name").textValue());
  }
}