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
package com.datasqrl.planner.hint;

import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.google.auto.service.AutoService;

/**
 * Defines a table as a workload which means it is expected to be executed in the database and we
 * don't generate an access function for it. It only exists to indicate how a consumer will query
 * the data for index selection and testing.
 */
public class WorkloadHint extends PlannerHint {

  public static final String HINT_NAME = "workload";

  protected WorkloadHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }

  @AutoService(Factory.class)
  public static class WorkLoadHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new WorkloadHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
