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
import java.util.List;

/**
 * Execution hints allow the user to assign a stage to a table/function definition and the
 * corresponding node in the DAG.
 */
public class EngineHint extends PlannerHint {

  public static final String HINT_NAME = "engine";
  public static final String OLD_HINT_NAME = "exec";

  protected EngineHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }

  public List<String> getStageNames() {
    return super.getOptions();
  }

  @AutoService(Factory.class)
  public static class ExecHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new EngineHint(source);
    }

    @Override
    public String getName() {
      return OLD_HINT_NAME;
    }
  }

  @AutoService(Factory.class)
  public static class EngineHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new EngineHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
