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
package com.datasqrl.eclipse;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.jface.text.Document;
import org.eclipse.jface.text.Region;
import org.eclipse.lsp4e.format.IFormatRegionsProvider;
import org.eclipse.lsp4e.internal.FormatRegionsProviderUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SqrlFormatRegionsProviderTest {

  private final Document document = new Document("X := SELECT\nid\nFROM t;\n");

  @AfterEach
  void resetPreference() {
    SqrlPreferences.store().setToDefault(SqrlPreferences.FORMAT_ON_SAVE);
  }

  @Test
  void givenDefaultPreferences_whenFormattingRegions_thenFormatsNothing() {
    assertThat(provider().getFormattingRegions(document)).isNull();
  }

  @Test
  void givenFormatOnSave_whenFormattingRegions_thenFormatsWholeDocument() {
    SqrlPreferences.store().setValue(SqrlPreferences.FORMAT_ON_SAVE, true);

    assertThat(provider().getFormattingRegions(document))
        .containsExactly(new Region(0, document.getLength()));
  }

  private static IFormatRegionsProvider provider() {
    var provider = FormatRegionsProviderUtil.lookup("com.datasqrl.eclipse.server");
    assertThat(provider).isInstanceOf(SqrlFormatRegionsProvider.class);
    return provider;
  }
}
