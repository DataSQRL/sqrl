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

import org.eclipse.jface.preference.BooleanFieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.IntegerFieldEditor;
import org.eclipse.jface.preference.RadioGroupFieldEditor;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;

/** Window | Preferences | DataSQRL */
public final class SqrlPreferencePage extends FieldEditorPreferencePage
    implements IWorkbenchPreferencePage {

  public SqrlPreferencePage() {
    super(GRID);
    setDescription(
        "How the SQRL language server is started. Changes apply when the server starts again, e.g."
            + " after all DataSQRL files were closed or Eclipse was restarted.");
  }

  @Override
  public void init(IWorkbench workbench) {
    setPreferenceStore(SqrlPreferences.store());
  }

  @Override
  protected void createFieldEditors() {
    var parent = getFieldEditorParent();
    addField(
        new RadioGroupFieldEditor(
            SqrlPreferences.LAUNCHER,
            "Start the language server",
            1,
            new String[][] {
              {"In the &Docker image of the CLI", SqrlPreferences.LAUNCHER_DOCKER},
              {"From the &jar of the CLI (also on Windows)", SqrlPreferences.LAUNCHER_JAR}
            },
            parent,
            true));
    addField(new StringFieldEditor(SqrlPreferences.DOCKER, "Docker &executable:", parent));
    addField(new StringFieldEditor(SqrlPreferences.DOCKER_IMAGE, "Docker &image:", parent));
    var jar = new FileFieldEditor(SqrlPreferences.JAR, "sqrl-cli.&jar:", parent);
    jar.setFileExtensions(new String[] {"*.jar"});
    addField(jar);
    addField(
        new FileFieldEditor(SqrlPreferences.JAVA, "Java 17-22 e&xecutable (default: JAVA_HOME):", parent));
    addField(
        new StringFieldEditor(
            SqrlPreferences.PACKAGE_FILES, "&Package files of all projects (comma-separated):", parent));
    addField(
        new IntegerFieldEditor(
            SqrlPreferences.CHECK_DELAY_MILLIS, "&Check delay after edits (ms):", parent));
    addField(
        new BooleanFieldEditor(
            SqrlPreferences.FULL_COMPILE_ON_SAVE, "Run the full &compilation on save", parent));
    addField(
        new BooleanFieldEditor(SqrlPreferences.FORMAT_ON_SAVE, "&Format scripts on save", parent));
  }
}
