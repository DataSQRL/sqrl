package com.datasqrl;

import java.io.File;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
@Testcontainers
@Disabled
public class PlannerIT {

//  @Container
//  public DockerComposeContainer<?> environment =
//      new DockerComposeContainer<>(new File("/Users/henneberger/test/sqrl-flink-sql-example/docker-compose.yml"))
//          .withExposedService("service_1", 1234)
//          .withExposedService("service_2", 5678)
//      ;

  @Test
  public void test() {

//    environment.start();
  }
}
