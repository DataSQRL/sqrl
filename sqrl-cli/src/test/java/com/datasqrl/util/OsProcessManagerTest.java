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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datasqrl.env.GlobalEnvironmentStore;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OsProcessManagerTest {

  @Mock private Process mockProcess;

  private OsProcessManager serviceManager;
  private Map<String, String> env;

  @BeforeEach
  void setUp() {
    env = new HashMap<>();
    env.put("POSTGRES_VERSION", "17");
    serviceManager = new OsProcessManager(env);
  }

  @AfterEach
  void tearDown() {
    // Clear global environment store that might have been set during tests
    GlobalEnvironmentStore.clear();
  }

  @Disabled
  @Test
  void givenKafkaAndPostgresHostsSet_whenStartServices_thenSkipsBothDependentServices()
      throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka");
    env.put("POSTGRES_HOST", "external-postgres");
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      when(mockPath.toAbsolutePath()).thenReturn(mockPath);
      when(mockPath.toString()).thenReturn("/mock/path");
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(mockPath)).thenReturn(true);
      filesMocked.when(() -> Files.list(mockPath)).thenReturn(Stream.of(mockPath));

      when(mockProcess.waitFor()).thenReturn(0);

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
              })) {

        // When
        serviceManager.startDependentServices();

        // Then - Should complete without starting any processes for dependent services
        // Note: ProcessBuilder may be created for directory ownership changes
        // We verify this by checking that environment variables were set as expected
        assertThat(GlobalEnvironmentStore.get("KAFKA_HOST")).isEqualTo("external-kafka");
        assertThat(GlobalEnvironmentStore.get("POSTGRES_HOST")).isEqualTo("external-postgres");
      }
    }
  }

  @Test
  void givenIOExceptionCreatingDirectories_whenStartDependentServices_thenThrowsRuntimeException()
      throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka");
    env.put("POSTGRES_HOST", "external-postgres");
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked
          .when(() -> Files.createDirectories(any(Path.class)))
          .thenThrow(new IOException("Permission denied"));

      // When & Then
      assertThatThrownBy(serviceManager::startDependentServices)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Service startup failed");
    }
  }

  @Disabled
  @Test
  void givenNoKafkaHost_whenStartDependentServices_thenStartsRedpanda() throws Exception {
    // Given
    env.put("POSTGRES_HOST", "external-postgres");
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      when(mockPath.toAbsolutePath()).thenReturn(mockPath);
      when(mockPath.toString()).thenReturn("/mock/path");
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
        serviceManager.startDependentServices();

        // Then
        assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);

        // Verify redpanda process was started (at least one ProcessBuilder was created)
        // We can't verify the exact command due to mocking limitations, but we can verify a process
        // was started

        // Verify that environment variables were set by checking they exist after the call
        assertThat(GlobalEnvironmentStore.get("KAFKA_HOST")).isEqualTo("localhost");
        assertThat(GlobalEnvironmentStore.get("KAFKA_PORT")).isEqualTo("9092");
        assertThat(GlobalEnvironmentStore.get("KAFKA_BOOTSTRAP_SERVERS"))
            .isEqualTo("localhost:9092");
      }
    }
  }

  @Disabled
  @Test
  void givenRedpandaProcessDies_whenStartDependentServices_thenThrowsException() throws Exception {
    // Given
    env.put("POSTGRES_HOST", "external-postgres"); // Skip postgres
    serviceManager = new OsProcessManager(env);

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
        assertThatThrownBy(() -> serviceManager.startDependentServices())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Service startup failed");
      }
    }
  }

  @Disabled
  @Test
  void givenNoPostgresHost_whenStartDependentServices_thenStartsPostgres() throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka"); // Skip redpanda
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      java.io.File mockFile = mock(java.io.File.class);
      when(mockPath.toAbsolutePath()).thenReturn(mockPath);
      when(mockPath.toString()).thenReturn("/mock/path");
      when(mockPath.toFile()).thenReturn(mockFile);
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
        serviceManager.startDependentServices();

        // Then
        assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);

        // Verify postgres environment variables are set by checking they exist after the call
        assertThat(GlobalEnvironmentStore.get("POSTGRES_HOST")).isEqualTo("localhost");
        assertThat(GlobalEnvironmentStore.get("POSTGRES_PORT")).isEqualTo("5432");
        assertThat(GlobalEnvironmentStore.get("POSTGRES_HOST")).isEqualTo("localhost");
        assertThat(GlobalEnvironmentStore.get("PGUSER")).isEqualTo("postgres");
        assertThat(GlobalEnvironmentStore.get("PGPASSWORD")).isEqualTo("postgres");
        assertThat(GlobalEnvironmentStore.get("PGDATABASE")).isEqualTo("datasqrl");
      }
    }
  }

  @Disabled
  @Test
  void givenEmptyPostgresDirectory_whenStartDependentServices_thenInitializesPostgres()
      throws Exception {
    // Given
    env.put("KAFKA_HOST", "external-kafka"); // Skip redpanda
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      java.io.File mockFile = mock(java.io.File.class);
      when(mockPath.toAbsolutePath()).thenReturn(mockPath);
      when(mockPath.toString()).thenReturn("/mock/path");
      when(mockPath.toFile()).thenReturn(mockFile);
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
        serviceManager.startDependentServices();

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

  @Disabled
  @Test
  void givenCustomEnvironmentVariables_whenStartDependentServices_thenSetsSystemProperties()
      throws Exception {
    env.put("KAFKA_HOST", "external-kafka");
    env.put("POSTGRES_HOST", "external-postgres");
    env.put("CUSTOM_PROPERTY", "custom_value");
    env.put("ANOTHER_PROPERTY", "another_value");
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Files> filesMocked = mockStatic(Files.class);
        MockedStatic<Paths> pathsMocked = mockStatic(Paths.class)) {

      Path mockPath = mock(Path.class);
      when(mockPath.toAbsolutePath()).thenReturn(mockPath);
      when(mockPath.toString()).thenReturn("/mock/path");
      pathsMocked.when(() -> Paths.get(anyString())).thenReturn(mockPath);
      pathsMocked.when(() -> Paths.get(anyString(), anyString())).thenReturn(mockPath);
      filesMocked.when(() -> Files.exists(mockPath)).thenReturn(true);
      filesMocked.when(() -> Files.list(mockPath)).thenReturn(Stream.of(mockPath));

      when(mockProcess.waitFor()).thenReturn(0);

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
              })) {

        // When
        serviceManager.startDependentServices();

        // Then - Check that environment variables were set by verifying they exist after the call
        assertThat(GlobalEnvironmentStore.get("CUSTOM_PROPERTY")).isEqualTo("custom_value");
        assertThat(GlobalEnvironmentStore.get("ANOTHER_PROPERTY")).isEqualTo("another_value");
        assertThat(GlobalEnvironmentStore.get("KAFKA_HOST")).isEqualTo("external-kafka");
        assertThat(GlobalEnvironmentStore.get("POSTGRES_HOST")).isEqualTo("external-postgres");
      }
    }
  }

  @Test
  void givenBuildDir_whenTeardown_thenMovesLogsAndSetsOwnership() throws Exception {
    // Given
    Path mockBuildDir = mock(Path.class);
    Path mockTargetDir = mock(Path.class);
    when(mockBuildDir.resolve("logs")).thenReturn(mockTargetDir);
    when(mockBuildDir.toAbsolutePath()).thenReturn(mockBuildDir);
    when(mockBuildDir.toString()).thenReturn("/build/dir");
    when(mockBuildDir.getParent()).thenReturn(mockBuildDir);

    env.put("BUILD_UID", "1000");
    env.put("BUILD_GID", "1000");
    serviceManager = new OsProcessManager(env);

    try (MockedStatic<Paths> pathsMocked = mockStatic(Paths.class);
        MockedStatic<org.apache.commons.io.FileUtils> fileUtilsMocked =
            mockStatic(org.apache.commons.io.FileUtils.class)) {

      Path mockLogPath = mock(Path.class);
      pathsMocked.when(() -> Paths.get("/tmp/logs")).thenReturn(mockLogPath);
      when(mockLogPath.toFile()).thenReturn(mock(java.io.File.class));
      when(mockTargetDir.toFile()).thenReturn(mock(java.io.File.class));

      when(mockProcess.waitFor()).thenReturn(0);

      try (MockedConstruction<ProcessBuilder> pbMocked =
          mockConstruction(
              ProcessBuilder.class,
              (mock, context) -> {
                when(mock.start()).thenReturn(mockProcess);
              })) {

        // When
        serviceManager.teardown(mockBuildDir);

        // Then
        fileUtilsMocked.verify(
            () ->
                org.apache.commons.io.FileUtils.moveDirectory(
                    any(java.io.File.class), any(java.io.File.class)));
        assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);
      }
    }
  }

  @Test
  void givenDirAndOwnership_whenSetOwnerForDir_thenExecutesChownCommand() throws Exception {
    // Given
    Path mockDir = mock(Path.class);
    when(mockDir.toAbsolutePath()).thenReturn(mockDir);
    when(mockDir.toString()).thenReturn("/test/dir");

    env.put("BUILD_UID", "1000");
    env.put("BUILD_GID", "1000");
    serviceManager = new OsProcessManager(env);

    when(mockProcess.waitFor()).thenReturn(0);

    try (MockedConstruction<ProcessBuilder> pbMocked =
        mockConstruction(
            ProcessBuilder.class,
            (mock, context) -> {
              when(mock.start()).thenReturn(mockProcess);
            })) {

      // When
      serviceManager.setOwnerForDir(mockDir);

      // Then
      assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);
    }
  }

  @Test
  void givenBlankOwnership_whenSetOwnerForDir_thenSkipsChownCommand() throws Exception {
    // Given
    Path mockDir = mock(Path.class);
    env.put("BUILD_UID", "");
    env.put("BUILD_GID", "");
    serviceManager = new OsProcessManager(env);

    try (MockedConstruction<ProcessBuilder> pbMocked = mockConstruction(ProcessBuilder.class)) {

      // When
      serviceManager.setOwnerForDir(mockDir);

      // Then
      assertThat(pbMocked.constructed()).isEmpty();
    }
  }

  @Test
  void givenChownFails_whenSetOwnerForDir_thenLogsWarning() throws Exception {
    // Given
    Path mockDir = mock(Path.class);
    when(mockDir.toAbsolutePath()).thenReturn(mockDir);
    when(mockDir.toString()).thenReturn("/test/dir");

    env.put("BUILD_UID", "1000");
    env.put("BUILD_GID", "1000");
    serviceManager = new OsProcessManager(env);

    when(mockProcess.waitFor()).thenReturn(1); // Failure

    try (MockedConstruction<ProcessBuilder> pbMocked =
        mockConstruction(
            ProcessBuilder.class,
            (mock, context) -> {
              when(mock.start()).thenReturn(mockProcess);
            })) {

      // When
      serviceManager.setOwnerForDir(mockDir);

      // Then
      assertThat(pbMocked.constructed()).hasSizeGreaterThan(0);
      // Note: We can't easily verify the log warning without additional mocking
    }
  }
}
