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
package com.datasqrl.compile;

import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqrlInjector;
import java.nio.file.Path;
import lombok.experimental.UtilityClass;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/** Creates the Spring context that holds all components of one compilation. */
@UtilityClass
public class CompilerContexts {

  /**
   * The directories of a compilation.
   *
   * @param workspaceDir the directory the compilation runs in; the compiler deletes its {@code
   *     sqrl_iceberg_data} subdirectory
   * @param projectRoot the directory of the project's package configuration and scripts
   * @param buildBaseDir the base of the build directory, which is excluded from the sources
   * @param buildDir the build directory, which is cleaned and filled with the preprocessed sources
   * @param targetDir the directory for the deployment artifacts
   */
  public record Directories(
      Path workspaceDir, Path projectRoot, Path buildBaseDir, Path buildDir, Path targetDir) {}

  /**
   * Creates and refreshes the context, and initializes the enabled engines.
   *
   * @return the context, which the caller must close
   */
  public static AnnotationConfigApplicationContext create(
      ErrorCollector errors,
      Directories dirs,
      PackageJson config,
      ExecutionGoal goal,
      boolean internalTestExec) {
    var springCtx = new AnnotationConfigApplicationContext();
    try {
      springCtx.registerBean(ErrorCollector.class, () -> errors);
      springCtx.registerBean("workspaceDir", Path.class, dirs::workspaceDir);
      springCtx.registerBean("projectRoot", Path.class, dirs::projectRoot);
      springCtx.registerBean("buildBaseDir", Path.class, dirs::buildBaseDir);
      springCtx.registerBean("buildDir", Path.class, dirs::buildDir);
      springCtx.registerBean("targetDir", Path.class, dirs::targetDir);
      springCtx.registerBean(PackageJson.class, () -> config);
      springCtx.registerBean(ExecutionGoal.class, () -> goal);
      springCtx.registerBean("internalTestExec", Boolean.class, () -> internalTestExec);
      springCtx.register(SqrlInjector.class);
      springCtx.refresh();

      springCtx.getBean(ExecutionEnginesHolder.class).initEnabledEngines();
      return springCtx;
    } catch (RuntimeException e) {
      springCtx.close();
      throw e;
    }
  }
}
