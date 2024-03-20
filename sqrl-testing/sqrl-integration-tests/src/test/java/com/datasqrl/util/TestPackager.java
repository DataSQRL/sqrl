//package com.datasqrl.util;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.SneakyThrows;
//import org.apache.flink.configuration.TaskManagerOptions;
//import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
//import org.testcontainers.containers.PostgreSQLContainer;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Map;
//
//public class TestPackager {
//
//  public static Path createPackageOverride(EmbeddedKafkaCluster kafka, PostgreSQLContainer testDatabase) {
//    Map overrideConfig = Map.of("engines",
//        Map.of("log", Map.of("connector",
//                Map.of("bootstrap.servers", "localhost:" + kafka.bootstrapServers().split(":")[1])),
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
//}
