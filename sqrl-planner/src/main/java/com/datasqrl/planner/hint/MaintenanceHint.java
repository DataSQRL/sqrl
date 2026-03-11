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

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.util.EnumUtil;
import com.google.auto.service.AutoService;
import java.util.Arrays;
import lombok.Getter;

@Getter
public class MaintenanceHint extends PlannerHint {

  public static final String HINT_NAME = "maintenance";

  public enum MaintenanceType {
    NONE,
    REGULAR
  }

  private final MaintenanceType maintenanceType;

  protected MaintenanceHint(ParsedObject<SqrlHint> source, MaintenanceType maintenanceType) {
    super(source, Type.DAG);
    this.maintenanceType = maintenanceType;
  }

  @AutoService(Factory.class)
  public static class MaintenanceHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().getOptions();

      if (arguments.size() == 1) {
        var optIndex = EnumUtil.getByName(MaintenanceType.class, arguments.get(0));

        if (optIndex.isPresent()) {
          return new MaintenanceHint(source, optIndex.get());
        }
      }

      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "Maintenance hint expect a single argument from list: %s.",
          Arrays.toString(MaintenanceType.values()));
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
