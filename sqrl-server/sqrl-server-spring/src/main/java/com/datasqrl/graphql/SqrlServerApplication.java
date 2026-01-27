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
package com.datasqrl.graphql;

import com.datasqrl.env.GlobalEnvironmentStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Main entry point for the Spring Boot GraphQL server application. Replaces the Vert.x-based
 * SqrlLauncher.
 */
@Slf4j
@SpringBootApplication
@EnableConfigurationProperties
public class SqrlServerApplication {

  public static void main(String[] args) {
    GlobalEnvironmentStore.putAll(System.getenv());
    SpringApplication.run(SqrlServerApplication.class, args);
  }
}
