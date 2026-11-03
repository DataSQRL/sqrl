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
import com.google.common.base.Preconditions;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Declassifies the listed tags, e.g. {@code /*+declassify(pii, classification) *\/}: the table's
 * value for each listed tag is what its doc string declares, or no value if it declares none, and
 * the listed tags are not validated against the inherited tags.
 */
@Getter
public class DeclassifyHint extends PlannerHint {

  public static final String HINT_NAME = "declassify";

  /** The normalized names of the declassified tags */
  private final Set<String> tagNames;

  protected DeclassifyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER);
    this.tagNames =
        source.get().options().stream()
            .map(String::trim)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @AutoService(Factory.class)
  public static class DeclassifyHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      Preconditions.checkArgument(
          !source.get().options().isEmpty(),
          "declassify hint requires at least one tag name, e.g. /*+declassify(pii) */");
      return new DeclassifyHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
