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
package org.junit.runners.model;

/*
 * testcontainers has a hard dependency on JUnit4, which we would like to avoid, migrating to
 * JUnit5 (aka junit jupiter). Until testcontainers updates their package to remove this dependency,
 * a workaround is needed to avoid the "org.junit.rules.TestRule not found" error. The two files
 * Statement.java and TestRule.java seem to accomplish this, following a comment in the github thread
 * where multiple people are encountering the same problem:
 * https://github.com/testcontainers/testcontainers-java/issues/970#issuecomment-625044008
 */
@SuppressWarnings("unused")
public class Statement {}
