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
package com.datasqrl.planner.contract.odcs;

import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.tags.TagValue;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * How an ODCS contract writes the tags of a table: without the {@code @} of a doc string, as {@code
 * gdpr} for a tag without a value and as {@code pii=raw} for a tag with one.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OdcsTags {

  private static final String VALUE_SEPARATOR = "=";

  /**
   * A tag as a contract lists it.
   *
   * @param value the value, or null for a tag that is written without one
   */
  public record ContractTag(String name, @Nullable String value) {

    @Override
    public String toString() {
      return value == null ? name : name + VALUE_SEPARATOR + value;
    }
  }

  public static ContractTag parse(String tag) {
    var name = StringUtils.substringBefore(tag, VALUE_SEPARATOR).trim();
    return new ContractTag(
        name,
        tag.contains(VALUE_SEPARATOR)
            ? StringUtils.substringAfter(tag, VALUE_SEPARATOR).trim()
            : null);
  }

  public static String write(TagValue tag) {
    return new ContractTag(tag.name(), tag.definition().isBoolean() ? null : tag.value())
        .toString();
  }

  /** The tags as a contract lists them, sorted by name. */
  public static List<String> write(TableTags tags) {
    return tags.values().stream().map(OdcsTags::write).toList();
  }
}
