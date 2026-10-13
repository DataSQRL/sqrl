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
package com.datasqrl.intellij;

import com.intellij.openapi.Disposable;
import com.intellij.openapi.application.PathManager;
import com.intellij.openapi.vfs.newvfs.impl.VfsRootAccess;
import com.intellij.testFramework.UsefulTestCase;
import com.intellij.testFramework.fixtures.CodeInsightTestFixture;
import com.intellij.testFramework.fixtures.IdeaTestFixtureFactory;
import com.intellij.testFramework.fixtures.impl.TempDirTestFixtureImpl;
import java.util.List;
import java.util.Objects;

/**
 * Completes package files with the JSON schema of DataSQRL package files. The files are not in the
 * content of the project, so that no language server starts.
 */
public class SqrlPackageSchemaTest extends UsefulTestCase {

  private CodeInsightTestFixture fixture;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    allowSchemaAccess(getTestRootDisposable());
    var factory = IdeaTestFixtureFactory.getFixtureFactory();
    fixture =
        factory.createCodeInsightFixture(
            factory.createFixtureBuilder(getName()).getFixture(), new TempDirTestFixtureImpl());
    fixture.setUp();
  }

  /**
   * Lets a test read the JSON schema of package files from the jar of the plugin. Tests may only
   * read files in some directories, e.g. the home directory, and the jar is in the sandbox in the
   * repository, which may be elsewhere, e.g. in a container.
   */
  static void allowSchemaAccess(Disposable disposable) {
    VfsRootAccess.allowRootAccess(
        disposable,
        Objects.requireNonNull(
            PathManager.getJarPathForClass(SqrlPackageSchemaProviderFactory.class)));
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      fixture.tearDown();
    } catch (Throwable e) {
      addSuppressedException(e);
    } finally {
      super.tearDown();
    }
  }

  public void testSectionsOfPackageFilesAreCompleted() throws Exception {
    open("project/test_package.json", "{\n  \"script\": {\"main\": \"main.sqrl\"},\n  \n}", "\n}");

    assertTrue(completions().toString(), completions().contains("enabled-engines"));
  }

  public void testPackageJsonOfDataSqrlIsCompletedInsteadOfAsNpm() throws Exception {
    open("project/package.json", "{\n  \"script\": {\"main\": \"main.sqrl\"},\n  \n}", "\n}");

    assertTrue(completions().toString(), completions().contains("enabled-engines"));
  }

  public void testNewPackageFileNextToScriptsIsCompleted() throws Exception {
    fixture.getTempDirFixture().createFile("project/main.sqrl", "X := SELECT 1;");
    open("project/new_package.json", "{\n  \n}", "\n}");

    assertTrue(completions().toString(), completions().contains("script"));
  }

  public void testPackageJsonOfNpmIsNotDataSqrl() throws Exception {
    fixture.getTempDirFixture().createFile("web/main.sqrl", "X := SELECT 1;");
    open("web/package.json", "{\n  \"dependencies\": {\"react\": \"19\"},\n  \n}", "\n}");

    assertFalse(completions().toString(), completions().contains("enabled-engines"));
  }

  /** Opens a file in the editor, with the caret before the last occurrence of a text. */
  private void open(String path, String content, String caretBefore) throws Exception {
    var file = fixture.getTempDirFixture().createFile(path, content);
    fixture.configureFromExistingVirtualFile(file);
    fixture.getEditor().getCaretModel().moveToOffset(content.lastIndexOf(caretBefore));
  }

  /** The names that are suggested at the caret, without quotes. */
  private List<String> completions() {
    fixture.completeBasic();
    var lookup = fixture.getLookupElementStrings();
    return lookup == null
        ? List.of()
        : lookup.stream().filter(Objects::nonNull).map(s -> s.replace("\"", "")).toList();
  }
}
