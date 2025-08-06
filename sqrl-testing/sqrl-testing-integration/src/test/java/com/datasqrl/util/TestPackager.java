/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// package com.datasqrl.util;
//
// import com.fasterxml.jackson.databind.ObjectMapper;
// import lombok.SneakyThrows;
// import org.apache.flink.configuration.TaskManagerOptions;
// import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
// import org.testcontainers.containers.PostgreSQLContainer;
//
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.util.Map;
//
// public class TestPackager {
//
//  public static Path createPackageOverride(EmbeddedKafkaCluster kafka, PostgreSQLContainer
// testDatabase) {
//    Map overrideConfig = Map.of("engines",
//        Map.of("log", Map.of("connector",
//                Map.of("bootstrap.servers", "localhost:" +
// kafka.bootstrapServers().split(":")[1])),
//            "database", toDbMap(testDatabase),
//            "streams", Map.of(
//                TaskManagerOptions.NETWORK_MEMORY_MIN.key(), "256mb",
//                TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "256mb",
//                TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "256mb")));
//    return writeJson(overrideConfig);
//  }
//
//  public static Map toDbMap(PostgreSQLContainer testDatabase) {
//    return Map.of("database", testDatabase.getDatabaseName(),
//        "dialect", "postgres",
//        "driver", testDatabase.getDriverClassName(),
//        "port", testDatabase.getMappedPort(5432),
//        "host", testDatabase.getHost(),
//        "user", testDatabase.getUsername(),
//        "password", testDatabase.getPassword(),
//        "url", testDatabase.getJdbcUrl());
//  }
//
//  @SneakyThrows
//  public static Path writeJson(Map overrideConfig) {
//    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
//
//    Path tmp = Files.createTempFile("pkjson", ".json");
//    mapper.writeValue(tmp.toFile(), overrideConfig);
//    return tmp;
//  }
// }
