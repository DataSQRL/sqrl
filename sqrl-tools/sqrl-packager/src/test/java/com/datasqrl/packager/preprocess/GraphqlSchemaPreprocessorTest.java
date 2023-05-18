package com.datasqrl.packager.preprocess;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.datasqrl.util.SqrlObjectMapper;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

class GraphqlSchemaPreprocessorTest {

  @Captor
  ArgumentCaptor<Path> pathCaptor;
  private Snapshot snapshot;

  @BeforeEach
  public void before(TestInfo testInfo) {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @SneakyThrows
  @Test
  public void testMutationProcessing() {
    Path schema = Paths.get(
        Resources.getResource("preprocessors/graphql/schema.graphqls").toURI());
    Path packageJson = Paths.get(
        Resources.getResource("preprocessors/graphql/package.json").toURI());

    GraphqlSchemaPreprocessor preprocessor = new GraphqlSchemaPreprocessor();

    pathCaptor = ArgumentCaptor.forClass(Path.class);

    ProcessorContext context = mock(ProcessorContext.class);
    SqrlConfig config = SqrlConfigCommons.fromFiles(ErrorCollector.root(), packageJson);
    when(context.getSqrlConfig()).thenReturn(config);

    preprocessor.loader(schema, context, ErrorCollector.root());

    verify(context).addDependency(pathCaptor.capture());

    // Get the captured argument
    Path capturedPath = pathCaptor.getValue();

    if (Files.isDirectory(capturedPath)) {
      Files.list(capturedPath).sorted().forEach(file -> {
        // Read the content of each file
        String content = null;
        try {
          content = Files.readString(file);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // Verify it can be read back
        TableDefinition schema1 = new Deserializer()
            .mapYAMLFile(file.toAbsolutePath(),
                TableDefinition.class);

        snapshot.addContent(content);
      });
    }

    snapshot.createOrValidate();
  }
}