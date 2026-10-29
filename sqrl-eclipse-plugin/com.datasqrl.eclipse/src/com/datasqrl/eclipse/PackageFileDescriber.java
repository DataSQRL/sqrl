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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.eclipse.core.runtime.QualifiedName;
import org.eclipse.core.runtime.content.IContentDescription;
import org.eclipse.core.runtime.content.ITextContentDescriber;

/**
 * Tells DataSQRL package files apart from other JSON files by their sections, e.g. from the
 * package.json of npm, whose {@code engines} section is left out for that reason.
 */
public final class PackageFileDescriber implements ITextContentDescriber {

  private static final Pattern DATASQRL_SECTION =
      Pattern.compile("\"(script|enabled-engines|connectors|compiler|test-runner)\"\\s*:");

  private static final int MAX_PACKAGE_FILE = 1 << 20;

  @Override
  public int describe(InputStream contents, IContentDescription description) throws IOException {
    return describe(new InputStreamReader(contents, StandardCharsets.UTF_8), description);
  }

  @Override
  public int describe(Reader contents, IContentDescription description) throws IOException {
    var text = new StringBuilder();
    var buffer = new char[8192];
    int read;
    while (text.length() < MAX_PACKAGE_FILE && (read = contents.read(buffer)) != -1) {
      text.append(buffer, 0, read);
    }
    return isDataSqrlPackage(text) ? VALID : INVALID;
  }

  static boolean isDataSqrlPackage(CharSequence text) {
    return DATASQRL_SECTION.matcher(text).find();
  }

  @Override
  public QualifiedName[] getSupportedOptions() {
    return new QualifiedName[0];
  }
}
