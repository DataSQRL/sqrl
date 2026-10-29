///
/// Copyright © 2021 DataSQRL (contact@datasqrl.com)
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as vscode from 'vscode';
import {
  DidChangeConfigurationNotification,
  ExecuteCommandRequest,
  LanguageClient,
  LanguageClientOptions,
} from 'vscode-languageclient/node';
import * as command from './command';
import { isDataSqrlPackage, isPackageName } from './packages';

const PACKAGE_FILES_COMMAND = 'sqrl.packageFiles';
const RUN_TESTS_COMMAND = 'sqrl.runTests';
const SELECTIONS = 'selectedPackageFiles';
const TITLE = 'DataSQRL';

interface PackageItem extends vscode.QuickPickItem {
  uri?: string;
}

interface PackageFiles {
  candidates?: string[];
  overlays?: string[];
  used?: string[];
}

let client: LanguageClient | undefined;
let context: vscode.ExtensionContext;
let status: vscode.StatusBarItem;

export async function activate(extensionContext: vscode.ExtensionContext): Promise<void> {
  context = extensionContext;
  status = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
  status.command = 'sqrl.selectPackageFiles';
  status.tooltip = 'The package files that the script is checked with';
  context.subscriptions.push(
    status,
    vscode.commands.registerCommand(RUN_TESTS_COMMAND, runTests),
    vscode.commands.registerCommand('sqrl.selectPackageFiles', selectPackageFiles),
    vscode.commands.registerCommand('sqrl.restartServer', restart),
    vscode.window.onDidChangeActiveTextEditor(() => updateStatus()),
    vscode.workspace.onWillSaveTextDocument((event) => {
      if (shouldFormatOnSave(event.document)) {
        event.waitUntil(format(event.document));
      }
    }),
    vscode.workspace.onDidChangeConfiguration(async (event) => {
      if (event.affectsConfiguration('sqrl.server')) {
        await restart();
      } else if (event.affectsConfiguration('sqrl')) {
        await sendSettings();
      }
    }),
  );
  await start();
}

export async function deactivate(): Promise<void> {
  await client?.stop();
}

function serverOptions(): command.ServerOptions {
  const config = vscode.workspace.getConfiguration('sqrl.server');
  return {
    launcher: config.get<command.Launcher>('launcher', 'docker'),
    docker: config.get('docker', 'docker'),
    dockerImage: config.get('dockerImage', 'datasqrl/cmd:latest'),
    jar: config.get('jar', ''),
    java: config.get('java', ''),
    command: config.get<string[]>('command', []),
  };
}

function settings(): Record<string, unknown> {
  const config = vscode.workspace.getConfiguration('sqrl');
  return {
    packageFiles: config.get<string[]>('packageFiles', []),
    checkDelayMillis: config.get('checkDelayMillis', 300),
    fullCompileOnSave: config.get('fullCompileOnSave', true),
    selectedPackageFiles: selections(),
  };
}

/** Unless VS Code formats on save itself, with `editor.formatOnSave` */
function shouldFormatOnSave(document: vscode.TextDocument): boolean {
  return (
    document.languageId === 'sqrl' &&
    vscode.workspace.getConfiguration('sqrl', document).get('formatOnSave', false) &&
    !vscode.workspace.getConfiguration('editor', document).get('formatOnSave', false)
  );
}

async function format(document: vscode.TextDocument): Promise<vscode.TextEdit[]> {
  const editor = vscode.window.visibleTextEditors.find((visible) => visible.document === document);
  const config = vscode.workspace.getConfiguration('editor', document);
  const options: vscode.FormattingOptions = {
    tabSize: Number(editor?.options.tabSize ?? config.get('tabSize', 4)),
    insertSpaces: Boolean(editor?.options.insertSpaces ?? config.get('insertSpaces', true)),
  };
  const edits = await vscode.commands.executeCommand<vscode.TextEdit[] | undefined>(
    'vscode.executeFormatDocumentProvider',
    document.uri,
    options,
  );
  return edits ?? [];
}

function selections(): Record<string, string[]> {
  return context.workspaceState.get<Record<string, string[]>>(SELECTIONS, {});
}

/** The directories of the projects, which Docker mounts: the workspace folders, else of the file */
function roots(): string[] {
  const folders = (vscode.workspace.workspaceFolders ?? [])
    .filter((folder) => folder.uri.scheme === 'file')
    .map((folder) => folder.uri.fsPath);
  const file = vscode.window.activeTextEditor?.document.uri;
  return folders.length > 0 || file?.scheme !== 'file' ? folders : [path.dirname(file.fsPath)];
}

/**
 * The working directory of the server, in the storage of the extension: files that checks create
 * at relative paths, e.g. a local Iceberg warehouse, end up there instead of in a project.
 */
function workDirectory(): string {
  const dir = path.join(context.globalStorageUri.fsPath, 'sqrl-lsp');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function start(): Promise<void> {
  const options = serverOptions();
  if (options.launcher === 'docker' && process.platform === 'win32') {
    void vscode.window.showErrorMessage(
      'On Windows, the language server cannot run in Docker yet, since it would see the files at' +
        ' other paths. Start it from the jar of the CLI with sqrl.server.launcher.',
    );
    return;
  }
  let executable: command.ProcessCommand;
  try {
    executable = command.server(options, roots(), process.env);
  } catch (error) {
    void vscode.window.showErrorMessage((error as Error).message);
    return;
  }
  // npm's package.json files are not sent to the server, which would otherwise check them
  const ignored = new Set<string>();
  const isIgnored = (document: vscode.TextDocument) => ignored.has(document.uri.toString());
  const clientOptions: LanguageClientOptions = {
    documentSelector: [
      { scheme: 'file', language: 'sqrl' },
      { scheme: 'file', pattern: '**/*{package,Package,PACKAGE}*.json' },
      { scheme: 'file', pattern: '**/*.{graphql,graphqls}' },
    ],
    initializationOptions: settings(),
    middleware: {
      didOpen: (document, next) => {
        if (isPackageName(document.fileName) && !isDataSqrlPackage(document.fileName, document.getText())) {
          ignored.add(document.uri.toString());
          return Promise.resolve();
        }
        return next(document);
      },
      didChange: (event, next) => (isIgnored(event.document) ? Promise.resolve() : next(event)),
      didSave: (document, next) => (isIgnored(document) ? Promise.resolve() : next(document)),
      didClose: (document, next) =>
        ignored.delete(document.uri.toString()) ? Promise.resolve() : next(document),
    },
  };
  client = new LanguageClient(
    'sqrl',
    TITLE,
    { command: executable.command, args: executable.args, options: { cwd: workDirectory() } },
    clientOptions,
  );
  try {
    await client.start();
  } catch (error) {
    void vscode.window.showErrorMessage(
      `The DataSQRL language server did not start: ${(error as Error).message}`,
    );
    return;
  }
  await updateStatus();
}

async function restart(): Promise<void> {
  const running = client;
  client = undefined;
  await running?.stop();
  await start();
}

async function sendSettings(): Promise<void> {
  await client?.sendNotification(DidChangeConfigurationNotification.type, { settings: settings() });
}

async function packageFiles(script: vscode.Uri): Promise<PackageFiles> {
  if (!client?.isRunning()) {
    return {};
  }
  const result = await client.sendRequest(ExecuteCommandRequest.type, {
    command: PACKAGE_FILES_COMMAND,
    arguments: [client.code2ProtocolConverter.asUri(script)],
  });
  return (result as PackageFiles | null) ?? {};
}

function activeScript(): vscode.Uri | undefined {
  const document = vscode.window.activeTextEditor?.document;
  return document?.languageId === 'sqrl' && document.uri.scheme === 'file'
    ? document.uri
    : undefined;
}

async function updateStatus(): Promise<void> {
  const script = activeScript();
  if (!script) {
    status.hide();
    return;
  }
  try {
    const base = (await packageFiles(script)).used?.[0];
    if (activeScript()?.toString() !== script.toString()) {
      return;
    }
    status.text = `$(package) ${base ? path.basename(vscode.Uri.parse(base).fsPath) : 'No package file'}`;
    status.show();
  } catch {
    status.hide();
  }
}

/** Lets the user select the base package file of the script and its overlays. */
async function selectPackageFiles(): Promise<void> {
  const script = activeScript();
  if (!script || !client?.isRunning()) {
    return;
  }
  const uri = client.code2ProtocolConverter.asUri(script);
  const files = await packageFiles(script);
  const candidates = files.candidates ?? [];
  if (candidates.length === 0) {
    void vscode.window.showInformationMessage(
      `No package file names ${path.basename(script.fsPath)} as its main script.`,
    );
    return;
  }
  const current = selections()[uri]?.[0] ?? files.used?.[0];
  const defaults = { label: 'Default', description: 'The package files that the server finds' };
  const base = await vscode.window.showQuickPick<PackageItem>(
    [defaults, ...candidates.map((candidate) => packageItem(candidate, candidate === current))],
    { title: `Base package file of ${path.basename(script.fsPath)}` },
  );
  if (!base) {
    return;
  }
  if (!base.uri) {
    await select(uri, []);
    return;
  }
  await select(uri, [base.uri]);
  const overlays = (await packageFiles(script)).overlays ?? [];
  if (overlays.length === 0) {
    return;
  }
  const selected = files.used ?? [];
  const picked = await vscode.window.showQuickPick(
    overlays.map((overlay) => packageItem(overlay, selected.includes(overlay))),
    { title: `Overlays of ${base.label}`, canPickMany: true },
  );
  if (picked) {
    await select(uri, [base.uri, ...picked.map((overlay) => overlay.uri)]);
  }
}

function packageItem(uri: string, picked: boolean): PackageItem & { uri: string } {
  const file = vscode.Uri.parse(uri);
  return {
    label: path.basename(file.fsPath),
    description: vscode.workspace.asRelativePath(file),
    picked,
    uri,
  };
}

async function select(script: string, files: string[]): Promise<void> {
  const selected = { ...selections() };
  if (files.length === 0) {
    delete selected[script];
  } else {
    selected[script] = files;
  }
  await context.workspaceState.update(SELECTIONS, selected);
  await sendSettings();
  await updateStatus();
}

/** Runs the tests of a project, for the code lenses above test tables, in a terminal. */
async function runTests(...uris: string[]): Promise<void> {
  if (uris.length === 0) {
    void vscode.window.showWarningMessage('The script is not part of a project with a package file.');
    return;
  }
  const files = uris.map((uri) => vscode.Uri.parse(uri));
  const folder = vscode.workspace.getWorkspaceFolder(files[0]);
  const test = command.test(
    serverOptions(),
    folder?.uri.fsPath,
    files.map((file) => file.fsPath),
  );
  const task = new vscode.Task(
    { type: 'sqrl' },
    folder ?? vscode.TaskScope.Workspace,
    'Tests',
    TITLE,
    new vscode.ProcessExecution(test.command, test.args, { cwd: test.cwd }),
  );
  await vscode.tasks.executeTask(task);
}
