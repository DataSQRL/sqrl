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
/**
 * This contains the old Flink RelNode to SqlNode mapping code. A lot of this is now deprecated
 * because we are constructing the Flink plan directly. However, some of the SqlNode utility methods
 * are still being used. Hence, this requires a careful refactor.
 *
 * <p>TODO: Refactor
 */
package com.datasqrl.engine.stream.flink.sql;
