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
import com.datasqrl.planner.tags.TagDefinitions;
import com.google.auto.service.AutoService;
import java.util.List;
import lombok.Getter;

/**
 * Declassifies the listed tags, e.g. {@code /*+declassify(pii, classification) *\/}: the table does
 * not inherit them, so it carries only the values its doc string declares for them.
 */
@Getter
public class DeclassifyHint extends PlannerHint {

  public static final String HINT_NAME = "declassify";

  /** The normalized names of the declassified tags */
  private final List<String> tagNames;

  protected DeclassifyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER);
    this.tagNames =
        source.get().options().stream().map(String::trim).map(TagDefinitions::normalize).toList();
  }

  @AutoService(Factory.class)
  public static class DeclassifyHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      if (source.get().options().isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "The declassify hint requires at least one tag name, e.g. /*+declassify(pii) */");
      }
      return new DeclassifyHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
