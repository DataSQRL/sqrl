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

import com.datasqrl.intellij.SqrlSettings.Launcher;
import com.intellij.openapi.fileChooser.FileChooserDescriptorFactory;
import com.intellij.openapi.options.Configurable;
import com.intellij.openapi.ui.TextFieldWithBrowseButton;
import com.intellij.ui.components.JBLabel;
import com.intellij.ui.components.JBRadioButton;
import com.intellij.ui.components.JBTextField;
import com.intellij.util.ui.FormBuilder;
import javax.swing.ButtonGroup;
import javax.swing.JComponent;
import javax.swing.JPanel;

/** The settings page, in Settings | Languages & Frameworks | DataSQRL. */
public final class SqrlConfigurable implements Configurable {

  private JBRadioButton docker;
  private JBRadioButton jar;
  private JBTextField dockerImage;
  private TextFieldWithBrowseButton jarPath;
  private TextFieldWithBrowseButton javaPath;

  @Override
  public String getDisplayName() {
    return SqrlLspClientDescriptor.NAME;
  }

  @Override
  public JComponent createComponent() {
    docker = new JBRadioButton("Docker image of the CLI");
    jar = new JBRadioButton("Jar of the CLI");
    var launchers = new ButtonGroup();
    launchers.add(docker);
    launchers.add(jar);
    docker.addItemListener(event -> updateEnabled());
    dockerImage = new JBTextField();
    jarPath = new TextFieldWithBrowseButton();
    jarPath.addBrowseFolderListener(
        null,
        FileChooserDescriptorFactory.createSingleFileDescriptor("jar")
            .withTitle("Select sqrl-cli.jar"));
    javaPath = new TextFieldWithBrowseButton();
    javaPath.addBrowseFolderListener(
        null,
        FileChooserDescriptorFactory.createSingleFileNoJarsDescriptor()
            .withTitle("Select the Java Executable"));
    if (javaPath.getTextField() instanceof JBTextField field) {
      field.getEmptyText().setText("The Java of JAVA_HOME, else the runtime of the IDE");
    }
    return FormBuilder.createFormBuilder()
        .addComponent(new JBLabel("Run the language server for .sqrl scripts from the"))
        .addComponent(docker)
        .addLabeledComponent("Image:", dockerImage)
        .addComponent(jar)
        .addLabeledComponent("Jar:", jarPath)
        .addLabeledComponent("Java:", javaPath)
        .addComponentFillVertically(new JPanel(), 0)
        .getPanel();
  }

  private void updateEnabled() {
    var isDocker = docker.isSelected();
    dockerImage.setEnabled(isDocker);
    jarPath.setEnabled(!isDocker);
    javaPath.setEnabled(!isDocker);
  }

  private Launcher launcher() {
    return docker.isSelected() ? Launcher.DOCKER : Launcher.JAR;
  }

  @Override
  public boolean isModified() {
    var settings = SqrlSettings.getInstance().getState();
    return launcher() != settings.launcher
        || !dockerImage.getText().equals(settings.dockerImage)
        || !jarPath.getText().equals(settings.jarPath)
        || !javaPath.getText().equals(settings.javaPath);
  }

  @Override
  public void apply() {
    var settings = SqrlSettings.getInstance().getState();
    settings.launcher = launcher();
    settings.dockerImage = dockerImage.getText().strip();
    settings.jarPath = jarPath.getText().strip();
    settings.javaPath = javaPath.getText().strip();
    SqrlLspIntegrationProvider.restartServers();
  }

  @Override
  public void reset() {
    var settings = SqrlSettings.getInstance().getState();
    docker.setSelected(settings.launcher == Launcher.DOCKER);
    jar.setSelected(settings.launcher == Launcher.JAR);
    dockerImage.setText(settings.dockerImage);
    jarPath.setText(settings.jarPath);
    javaPath.setText(settings.javaPath);
    updateEnabled();
  }
}
