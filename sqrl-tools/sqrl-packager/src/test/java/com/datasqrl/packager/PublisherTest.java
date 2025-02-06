package com.datasqrl.packager;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.repository.PublishRepository;

import lombok.SneakyThrows;

@ExtendWith(MockitoExtension.class)
class PublisherTest {

  private Publisher publisher;

  @Mock
  private ErrorCollector errorCollector;

  @Mock
  private PublishRepository publishRepository;

  @BeforeEach
  void setUp() {
    publisher = new Publisher(errorCollector);
  }

  @Test
  @SneakyThrows
  void testPublishSuccess_validPackageJson() {
    // Arrange
    Path validPackagePath = Path.of(getClass().getClassLoader().getResource("publisher/valid").toURI());

    when(publishRepository.publish(any(Path.class), any())).thenReturn(true);
    when(errorCollector.withConfig(any(Path.class))).thenReturn(errorCollector);
    when(errorCollector.withConfig(any(String.class))).thenReturn(errorCollector);

    // Act
    PackageConfiguration result = publisher.publish(validPackagePath, publishRepository);

    // Assert
    assertNotNull(result, "The package should have been published successfully.");
    verify(errorCollector, never()).checkFatal(anyBoolean(), anyString(), any(Object[].class));
  }

  @Test
  @SneakyThrows
  void testPublishFailure_invalidPackageJson() {
    // Arrange
    Path validPackagePath = Path.of(getClass().getClassLoader().getResource("publisher/invalid").toURI());

    when(errorCollector.withConfig(any(Path.class))).thenReturn(errorCollector);
    when(errorCollector.abortOnFatal(any(Boolean.class))).thenReturn(errorCollector);
    when(errorCollector.exception(any(String.class), any())).thenReturn(new RuntimeException("Invalid package.json"));

    // Act & Assert
    assertThrows(RuntimeException.class, () -> {
      PackageConfiguration result = publisher.publish(validPackagePath, publishRepository);
    }, "Expected an exception due to invalid package.json");

    verify(errorCollector, times(1)).fatal(any(String.class), eq("$.package: required property 'name' not found"), any());
  }

}