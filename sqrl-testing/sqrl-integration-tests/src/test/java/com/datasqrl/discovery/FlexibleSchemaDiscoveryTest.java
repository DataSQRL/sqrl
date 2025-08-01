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
package com.datasqrl.discovery;

import static com.datasqrl.AbstractAssetSnapshotTest.getDisplayName;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.loaders.schema.SchemaLoaderImpl;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlexibleSchemaDiscoveryTest {

  public static final Path FILES_DIR =
      AbstractAssetSnapshotTest.getResourcesDirectory("discoveryfiles");

  ErrorCollector errors = ErrorCollector.root();
  SchemaLoader schemaLoader;

  public FlexibleSchemaDiscoveryTest() {
    var resourceResolver = new FileResourceResolver(FILES_DIR);
    schemaLoader = new SchemaLoaderImpl(resourceResolver, errors);
  }

  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void scripts(Path file) {
    assertThat(Files.exists(file)).isTrue();
    String filename = file.getFileName().toString();
    Optional<SchemaConversionResult> result = schemaLoader.loadSchema(filename, filename);
    assertThat(result.isPresent()).isTrue();
    var snapshot = Snapshot.of(getDisplayName(file), getClass());
    snapshot.addContent(result.get().type().getFullTypeString(), "type");
    snapshot.addContent(result.get().connectorOptions().toString(), "connector");
    snapshot.createOrValidate();
  }

  static class DataFiles implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return Files.list(FILES_DIR).filter(Files::isRegularFile).map(Arguments::of);
    }
  }
}
