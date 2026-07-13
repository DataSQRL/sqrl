/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.loader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * Overrides the original version from the Flink repo, so {@link PlannerModule#getInstance()} calls
 * in Flink does not trip on {@code flink-table-planner.jar} not being on the classpath. That is
 * deliberate, cause the SQRL docker image does not follow a full Flink distribution but runs
 * everything in a single JVM process.
 *
 * <p>This only applies for the {@code cmd} Docker image, real deployments using the Flink SQL
 * runner are unaffected by this change.
 */
@Internal
@SuppressWarnings("unused")
public class PlannerModule {

  private static final String[] OWNER_CLASSPATH =
      Stream.concat(
              Arrays.stream(CoreOptions.PARENT_FIRST_LOGGING_PATTERNS),
              Stream.of(
                  // These packages are shipped either by
                  // flink-table-runtime or flink-dist itself
                  "org.codehaus.janino",
                  "org.codehaus.commons",
                  "org.apache.commons.lang3",
                  "org.apache.commons.math3",
                  "org.apache.commons.text",
                  // with hive dialect, hadoop jar should be in classpath,
                  // also, we should make it loaded by owner classloader,
                  // otherwise, it'll throw class not found exception
                  // when initialize HiveParser which requires hadoop
                  "org.apache.hadoop"))
          .toArray(String[]::new);

  private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.flink"};

  private final PlannerComponentClassLoader submoduleClassLoader;

  private PlannerModule() {
    final ClassLoader flinkClassLoader = PlannerModule.class.getClassLoader();
    final URL jarLocation = PlannerModule.class.getProtectionDomain().getCodeSource().getLocation();

    this.submoduleClassLoader =
        new PlannerComponentClassLoader(
            new URL[] {jarLocation}, flinkClassLoader, OWNER_CLASSPATH, COMPONENT_CLASSPATH);
  }

  public URLClassLoader getSubmoduleClassLoader() {
    return this.submoduleClassLoader;
  }

  public void addUrlToClassLoader(URL url) {
    // add the url to component url
    this.submoduleClassLoader.addURL(url);
  }

  // Singleton lazy initialization

  private static class PlannerComponentsHolder {
    private static final PlannerModule INSTANCE = new PlannerModule();
  }

  public static PlannerModule getInstance() {
    return PlannerComponentsHolder.INSTANCE;
  }

  // load methods for various components provided by the planner

  public ExecutorFactory loadExecutorFactory() {
    return FactoryUtil.discoverFactory(
        this.submoduleClassLoader, ExecutorFactory.class, ExecutorFactory.DEFAULT_IDENTIFIER);
  }

  public PlannerFactory loadPlannerFactory() {
    return FactoryUtil.discoverFactory(
        this.submoduleClassLoader, PlannerFactory.class, PlannerFactory.DEFAULT_IDENTIFIER);
  }

  /**
   * A class loader extending {@link ComponentClassLoader} which overwrites method{@link #addURL} to
   * enable it can add url to component classloader.
   */
  private static class PlannerComponentClassLoader extends ComponentClassLoader {

    public PlannerComponentClassLoader(
        URL[] classpath,
        ClassLoader ownerClassLoader,
        String[] ownerFirstPackages,
        String[] componentFirstPackages) {
      super(classpath, ownerClassLoader, ownerFirstPackages, componentFirstPackages, Map.of());
    }

    @Override
    public void addURL(URL url) {
      super.addURL(url);
    }
  }
}
