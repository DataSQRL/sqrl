package com.datasqrl.packager;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJsonImpl;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Preprocessors;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import java.nio.file.Files;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
public class PreprocessorsTest {

  @InjectMocks
  private Preprocessors preprocessors;

  @Mock
  private Preprocessor firstPreprocessor;
  @Mock
  private Preprocessor secondPreprocessor;

  private Preprocessors.PreprocessorsContext context;
  private Path rootDir = Paths.get("/test");
  private Path buildDir = Paths.get("/build");

  @SneakyThrows
  @BeforeEach
  public void setUp() {
    Set<Preprocessor> preprocessorSet = new HashSet<>();
    preprocessorSet.add(firstPreprocessor);
    preprocessorSet.add(secondPreprocessor);
    preprocessors = new Preprocessors(preprocessorSet);

    context = Preprocessors.PreprocessorsContext.builder()
        .rootDir(rootDir)
        .buildDir(buildDir)
        .config(new PackageJsonImpl())
        .profiles(new String[]{})
        .errors(ErrorCollector.root())
        .build();
  }

  @Test
  public void testMultiplePreprocessorsForSingleFile() {
    Path fileToProcess = Paths.get("/test/src/File.java");
    when(firstPreprocessor.getPattern()).thenReturn(Pattern.compile(".*\\.java"));
    when(secondPreprocessor.getPattern()).thenReturn(Pattern.compile("File.*"));

    preprocessors.processUserFiles(Stream.of(fileToProcess).collect(Collectors.toList()), context);

    verify(firstPreprocessor).processFile(eq(fileToProcess), any(), any());
    verify(secondPreprocessor).processFile(eq(fileToProcess), any(), any());
  }

  @SneakyThrows
  @Test
  public void testExcludedDirectories() {
    Path excludedDir = Paths.get("/test/build");
    Path includedFile = Paths.get("/test/src/File.java");

    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
      mockedFiles.when(() -> Files.walk(rootDir))
          .thenReturn(Stream.of(excludedDir, includedFile));
      assertTrue(preprocessors.handle(context));
      verify(firstPreprocessor, never()).processFile(eq(excludedDir), any(), any());
      verify(secondPreprocessor, never()).processFile(eq(excludedDir), any(), any());
    }
  }
}
