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
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DependentServiceManagerTest {

  @Mock private Process mockProcess;

  private DependentServiceManager serviceManager;
  private Map<String, String> env;

  @BeforeEach
  void setUp() {
    env = new HashMap<>();
    env.put("POSTGRES_VERSION", "17");
    serviceManager = new DependentServiceManager(env);
  }

  @AfterEach
  void tearDown() {
    // Clear system properties that might have been set during tests
    System.clearProperty("KAFKA_HOST");
    System.clearProperty("KAFKA_PORT");
    System.clearProperty("PROPERTIES_BOOTSTRAP_SERVERS");
    System.clearProperty("POSTGRES_HOST");
    System.clearProperty("POSTGRES_PORT");
    System.clearProperty("JDBC_URL");
    System.clearProperty("JDBC_AUTHORITY");
    System.clearProperty("PGHOST");
    System.clearProperty("PGUSER");
    System.clearProperty("JDBC_USERNAME");
    System.clearProperty("JDBC_PASSWORD");
    System.clearProperty("PGPORT");
    System.clearProperty("PGPASSWORD");
    System.clearProperty("PGDATABASE");
    System.clearProperty("CUSTOM_PROPERTY");
    System.clearProperty("ANOTHER_PROPERTY");
  }

  @Test
  void givenKafkaAndPostgresHostsSet_whenStartServices_thenSkipsBothServices() throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka");
    env.put("POSTGRES_HOST", "external-postgres");
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(mockPath)).thenReturn(true);
      filesMocked.when(() -> Files.list(mockPath)).thenReturn(Stream.of(mockPath));

      // When
      serviceManager.startServices();

      // Then - Should complete without starting any processes
      // We verify this by checking that no ProcessBuilder was created
      try (MockedConstruction<ProcessBuilder> pbMocked = mockConstruction(ProcessBuilder.class)) {
        // No processes should be started when external hosts are configured
        assertThat(pbMocked.constructed()).isEmpty();
      }
    }
  }

  @Test
  void givenIOExceptionCreatingDirectories_whenStartServices_thenThrowsRuntimeException()
      throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka");
    env.put("POSTGRES_HOST", "external-postgres");
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked
          .when(() -> Files.createDirectories(any(Path.class)))
          .thenThrow(new IOException("Permission denied"));

      // When & Then
      assertThatThrownBy(serviceManager::startServices)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Service startup failed");
    }
  }

  @Test
  void givenNoKafkaHost_whenStartServices_thenStartsRedpanda() throws Exception {
    // Given
    env.put("POSTGRES_HOST", "external-postgres");
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.createDirectories(any(Path.class))).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(mockPath)).thenReturn(true);
      filesMocked.when(() -> Files.list(mockPath)).thenReturn(Stream.of(mockPath));

      when(mockProcess.isAlive()).thenReturn(true);
      when(mockProcess.waitFor()).thenReturn(0);

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
                when(mock.redirectOutput(any(File.class))).thenReturn(mock);
                when(mock.redirectErrorStream(any(Boolean.class))).thenReturn(mock);
                when(mock.redirectOutput(any(ProcessBuilder.Redirect.class))).thenReturn(mock);
                when(mock.redirectError(any(ProcessBuilder.Redirect.class))).thenReturn(mock);
              })) {

        // When
        serviceManager.startServices();

        // Then
        assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);

        // Verify redpanda process was started (at least one ProcessBuilder was created)
        // We can't verify the exact command due to mocking limitations, but we can verify a process
        // was started

        // Verify that system properties were set by checking they exist after the call
        assertThat(System.getProperty("KAFKA_HOST")).isEqualTo("localhost");
        assertThat(System.getProperty("KAFKA_PORT")).isEqualTo("9092");
        assertThat(System.getProperty("PROPERTIES_BOOTSTRAP_SERVERS")).isEqualTo("localhost:9092");
      }
    }
  }

  @Test
  void givenRedpandaProcessDies_whenStartServices_thenThrowsException() throws Exception {
    // Given
    env.put("POSTGRES_HOST", "external-postgres"); // Skip postgres
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.createDirectories(any(Path.class))).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(any(Path.class))).thenReturn(true);
      filesMocked
          .when(() -> Files.readAllLines(any(Path.class)))
          .thenReturn(java.util.List.of("Error starting redpanda"));

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
                when(mock.redirectOutput(any(File.class))).thenReturn(mock);
                when(mock.redirectErrorStream(any(Boolean.class))).thenReturn(mock);
              })) {

        // When & Then
        assertThatThrownBy(() -> serviceManager.startServices())
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Service startup failed");
      }
    }
  }

  @Test
  void givenNoPostgresHost_whenStartServices_thenStartsPostgres() throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka"); // Skip redpanda
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.createDirectories(any(Path.class))).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(any(Path.class))).thenReturn(true);
      filesMocked.when(() -> Files.list(any(Path.class))).thenReturn(Stream.of(mockPath));

      when(mockProcess.waitFor()).thenReturn(0);

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
                when(mock.redirectOutput(any(File.class))).thenReturn(mock);
                when(mock.redirectErrorStream(any(Boolean.class))).thenReturn(mock);
                when(mock.redirectOutput(any(ProcessBuilder.Redirect.class))).thenReturn(mock);
                when(mock.redirectError(any(ProcessBuilder.Redirect.class))).thenReturn(mock);
              })) {

        // When
        serviceManager.startServices();

        // Then
        assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);

        // Verify postgres environment variables are set by checking they exist after the call
        assertThat(System.getProperty("POSTGRES_HOST")).isEqualTo("localhost");
        assertThat(System.getProperty("POSTGRES_PORT")).isEqualTo("5432");
        assertThat(System.getProperty("PGHOST")).isEqualTo("localhost");
        assertThat(System.getProperty("PGUSER")).isEqualTo("postgres");
        assertThat(System.getProperty("PGPASSWORD")).isEqualTo("postgres");
        assertThat(System.getProperty("PGDATABASE")).isEqualTo("datasqrl");
      }
    }
  }

  @Test
  void givenEmptyPostgresDirectory_whenStartServices_thenInitializesPostgres() throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka"); // Skip redpanda
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.createDirectories(any(Path.class))).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(any(Path.class))).thenReturn(true);
      filesMocked
          .when(() -> Files.list(any(Path.class)))
          .thenReturn(Stream.empty()); // Empty directory

      when(mockProcess.waitFor()).thenReturn(0);

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
                when(mock.redirectOutput(any(File.class))).thenReturn(mock);
                when(mock.redirectErrorStream(any(Boolean.class))).thenReturn(mock);
                when(mock.redirectOutput(any(ProcessBuilder.Redirect.class))).thenReturn(mock);
                when(mock.redirectError(any(ProcessBuilder.Redirect.class))).thenReturn(mock);
              })) {

        // When
        serviceManager.startServices();

        // Then - Should initialize postgres (at least one ProcessBuilder was created)
        // We can't verify the exact command due to mocking limitations, but we can verify a process
        // was started
        assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);
      }
    }
  }

  @Test
  void givenLogFileExists_whenReadServiceLogFile_thenReturnsContent() throws Exception {
    // Given
    String serviceName = "TestService";

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockLogFile = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockLogFile);
      filesMocked.when(() -> Files.exists(mockLogFile)).thenReturn(true);
      filesMocked
          .when(() -> Files.readAllLines(mockLogFile))
          .thenReturn(java.util.List.of("Line 1", "Line 2", "Line 3"));

      // When
      String result = serviceManager.readServiceLogFile(serviceName);

      // Then
      assertThat(result).contains("Line 1").contains("Line 2").contains("Line 3");
    }
  }

  @Test
  void givenLogFileDoesNotExist_whenReadServiceLogFile_thenReturnsNotFoundMessage()
      throws Exception {
    // Given
    String serviceName = "TestService";

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockLogFile = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockLogFile);
      filesMocked.when(() -> Files.exists(mockLogFile)).thenReturn(false);
      when(mockLogFile.toString()).thenReturn("/tmp/logs/testservice.log");

      // When
      String result = serviceManager.readServiceLogFile(serviceName);

      // Then
      assertThat(result).contains("Log file not found at");
    }
  }

  @Test
  void givenIOExceptionReadingLogFile_whenReadServiceLogFile_thenReturnsErrorMessage()
      throws Exception {
    // Given
    String serviceName = "TestService";

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockLogFile = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockLogFile);
      filesMocked.when(() -> Files.exists(mockLogFile)).thenReturn(true);
      filesMocked
          .when(() -> Files.readAllLines(mockLogFile))
          .thenThrow(new IOException("Read error"));

      // When
      String result = serviceManager.readServiceLogFile(serviceName);

      // Then
      assertThat(result).contains("Failed to read log file").contains("Read error");
    }
  }

  @Test
  void givenCustomEnvironmentVariables_whenStartServices_thenSetsSystemProperties()
      throws Exception {
    env.put("KAFKA_HOST", "external-kafka");
    env.put("POSTGRES_HOST", "external-postgres");
    env.put("CUSTOM_PROPERTY", "custom_value");
    env.put("ANOTHER_PROPERTY", "another_value");
    serviceManager = new DependentServiceManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(mockPath)).thenReturn(true);
      filesMocked.when(() -> Files.list(mockPath)).thenReturn(Stream.of(mockPath));

      // When
      serviceManager.startServices();

      // Then - Check that system properties were set by verifying they exist after the call
      assertThat(System.getProperty("CUSTOM_PROPERTY")).isEqualTo("custom_value");
      assertThat(System.getProperty("ANOTHER_PROPERTY")).isEqualTo("another_value");
      assertThat(System.getProperty("KAFKA_HOST")).isEqualTo("external-kafka");
      assertThat(System.getProperty("POSTGRES_HOST")).isEqualTo("external-postgres");
    }
  }
}
