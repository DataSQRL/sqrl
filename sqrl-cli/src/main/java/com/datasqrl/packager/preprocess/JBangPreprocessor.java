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
package com.datasqrl.packager.preprocess;

import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.JBangRunner;
import com.google.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FilenameUtils;

@RequiredArgsConstructor(onConstructor_ = @Inject)
@Slf4j
public class JBangPreprocessor extends UdfManifestPreprocessor {

  private final JBangRunner jBangRunner;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    var srcFileName = file.getFileName().toString();

    if (!jBangRunner.isJBangAvailable() || skipFile(srcFileName)) {
      return;
    }

    try {
      var targetFileName = FilenameUtils.removeExtension(srcFileName) + ".jar";
      var targetPath = ctx.libDir().resolve(targetFileName);

      jBangRunner.exportLocalJar(file, targetPath);
      extractSqrlManifests(targetPath, ctx);

    } catch (ExecuteException e) {
      log.warn("JBang export failed with exit code: {} for file: {}", e.getExitValue(), file);
    } catch (IOException e) {
      log.warn("Could not execute JBang for file: " + file, e);
    }
  }

  private boolean skipFile(String fileName) {
    return !fileName.endsWith(".java") && !fileName.endsWith(".kt");
  }
}
