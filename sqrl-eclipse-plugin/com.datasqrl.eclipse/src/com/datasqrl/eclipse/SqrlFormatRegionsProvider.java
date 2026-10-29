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

import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.IRegion;
import org.eclipse.lsp4e.format.IFormatRegionsProvider;

/** Formats whole documents on save, if enabled, which LSP4E asks of the server before saving. */
public final class SqrlFormatRegionsProvider implements IFormatRegionsProvider {

  @Override
  public IRegion[] getFormattingRegions(IDocument document) {
    return SqrlPreferences.store().getBoolean(SqrlPreferences.FORMAT_ON_SAVE)
        ? IFormatRegionsProvider.allLines(document)
        : null;
  }
}
