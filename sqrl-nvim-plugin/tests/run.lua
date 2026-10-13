--[[

    Copyright © 2021 DataSQRL (contact@datasqrl.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

]]
-- Tests the plugin in Neovim without the configuration of the user:
--
--   nvim --clean --headless -l sqrl-nvim-plugin/tests/run.lua
--
-- The tests of the language server start it from the jar of the CLI in `SQRL_CLI_JAR`, or else from
-- `sqrl-cli/target/sqrl-cli.jar` if it was built, and are skipped otherwise.
local plugin_dir = vim.fn.fnamemodify(debug.getinfo(1, 'S').source:sub(2), ':p:h:h')
vim.opt.runtimepath:prepend(plugin_dir)
vim.cmd('filetype plugin on')
vim.cmd('syntax on')
vim.cmd.runtime('plugin/sqrl.lua')

-- The log and the cache of the user's Neovim are not written
vim.lsp.log.set_level(vim.log.levels.OFF)
vim.env.XDG_CACHE_HOME = vim.fn.tempname()

local project = require('sqrl.project')
local command = require('sqrl.command')

local TIMEOUT = 120000
local failures = 0

local function test(name, fn)
  local ok, err = xpcall(fn, debug.traceback)
  print((ok and 'ok      ' or 'FAILED  ') .. name)
  if not ok then
    print(err)
    failures = failures + 1
  end
end

local function check(condition, message)
  if not condition then
    error(message, 2)
  end
end

--- Waits until a function returns a value, while Neovim handles its events.
local function await(what, fn)
  local result
  local done = vim.wait(TIMEOUT, function()
    result = fn()
    return result ~= nil and result ~= false
  end, 100)
  check(done, 'Timed out waiting for ' .. what)
  return result
end

local function write(path, text)
  vim.fn.mkdir(vim.fs.dirname(path), 'p')
  vim.fn.writefile(vim.split(text, '\n', { plain = true }), path, 'b')
end

--- The 0-based line and byte column of the first occurrence of a text in a buffer.
local function find(bufnr, text)
  for i, line in ipairs(vim.api.nvim_buf_get_lines(bufnr, 0, -1, false)) do
    local col = line:find(text, 1, true)
    if col then
      return i - 1, col - 1
    end
  end
  error('Not found: ' .. text)
end

local function request(bufnr, method, line, character)
  local params = {
    textDocument = { uri = vim.uri_from_bufnr(bufnr) },
    position = { line = line, character = character },
  }
  local responses = vim.lsp.buf_request_sync(bufnr, method, params, TIMEOUT) or {}
  for _, response in pairs(responses) do
    if response.result then
      return response.result
    end
  end
end

-- The real path, since Neovim resolves the paths of buffers, e.g. on macOS /var to /private/var
local root = vim.fn.tempname()
vim.fn.mkdir(root, 'p')
root = vim.uv.fs_realpath(root)

test('scripts are detected', function()
  check(vim.filetype.match({ filename = 'main.sqrl' }) == 'sqrl', 'not detected')
end)

test('package files of DataSQRL are told apart from those of npm', function()
  write(root .. '/files/project/main.sqrl', 'X := SELECT 1;\n')
  write(root .. '/files/project/orders_test_package.json', '{"script": {"main": "main.sqrl"}}\n')
  write(root .. '/files/project/new_package.json', '{}\n')
  write(root .. '/files/project/data.json', '{"script": {}}\n')
  write(root .. '/files/web/main.sqrl', 'X := SELECT 1;\n')
  write(root .. '/files/web/package.json', '{"dependencies": {"react": "19"}}\n')
  write(root .. '/files/lib/package.json', '{}\n')

  check(project.is_package_file(root .. '/files/project/orders_test_package.json'), 'sections')
  check(project.is_package_file(root .. '/files/project/new_package.json'), 'next to scripts')
  check(not project.is_package_file(root .. '/files/project/data.json'), 'not named so')
  check(not project.is_package_file(root .. '/files/web/package.json'), 'npm')
  check(not project.is_package_file(root .. '/files/lib/package.json'), 'not next to scripts')
  check(
    project.project_dir(root .. '/files/project/api/schema.graphqls') == root .. '/files/project',
    'project of a schema'
  )
  check(project.project_dir(root .. '/files/lib/schema.graphqls') == nil, 'not a project')
end)

test('the server starts from the jar, in Docker, or with a command', function()
  local jar = command.build({ jar = '/cli/sqrl-cli.jar', java = '/jdk/bin/java' }, '/repo')
  check(jar[1] == '/jdk/bin/java', 'java')
  check(vim.list_contains(jar, '--add-opens=java.base/java.lang=ALL-UNNAMED'), 'JVM options')
  check(table.concat(vim.list_slice(jar, #jar - 2), ' ') == '-jar /cli/sqrl-cli.jar lsp', 'jar')

  local docker =
    command.build({ docker = 'docker', docker_image = 'datasqrl/cmd:latest' }, '/repo')
  check(
    table.concat(docker, ' ')
      == 'docker run --interactive --rm --tmpfs /workspace --volume /repo:/repo'
        .. ' datasqrl/cmd:latest lsp',
    table.concat(docker, ' ')
  )
  check(command.build({ cmd = { 'sqrl', 'lsp' } }, '/repo')[1] == 'sqrl', 'command')
  -- The tests of a project run like the server
  local files = { '/repo/project/package.json' }
  -- In the Docker image of the CLI, which has the services that tests need
  local docker_test =
    command.test({ docker = 'docker', docker_image = 'datasqrl/cmd:latest' }, '/repo', files)
  check(
    table.concat(docker_test.cmd, ' ')
        == 'docker run --rm --interactive --tty --volume /repo:/workspace'
          .. ' datasqrl/cmd:latest test project/package.json'
      and docker_test.cwd == '/repo',
    vim.inspect(docker_test)
  )
  -- With a jar of the CLI, which runs in the image
  local jar_test = command.test({
    jar = '/cli/sqrl-cli.jar',
    docker = 'docker',
    docker_image = 'datasqrl/cmd:latest',
  }, '/repo', files)
  check(
    table.concat(jar_test.cmd, ' ')
      == 'docker run --rm --interactive --tty --volume /repo:/workspace'
        .. ' --volume /cli/sqrl-cli.jar:/opt/sqrl/sqrl-cli.jar'
        .. ' datasqrl/cmd:latest test project/package.json',
    vim.inspect(jar_test)
  )
  local cmd_test = command.test({ cmd = { 'sqrl', 'lsp' } }, '/repo', files)
  check(table.concat(cmd_test.cmd, ' ') == 'sqrl test package.json', vim.inspect(cmd_test))
  -- Files that checks create at relative paths end up in the cache, not in a project
  local workdir = command.workdir()
  check(vim.fn.isdirectory(workdir) == 1, workdir)
  check(vim.startswith(workdir, vim.env.XDG_CACHE_HOME), workdir)
end)

test('scripts are highlighted', function()
  local path = root .. '/syntax/highlighting.sqrl'
  write(
    path,
    table.concat({
      'IMPORT connectors.police_{{variant}} AS ctl;',
      '/*+primary_key(id) */',
      "Orders := SELECT o.id, COUNT(*) AS n FROM ctl.Orders o WHERE o.status = 'open'; -- latest",
      'Orders.items(lim INT) := SELECT * FROM Items i WHERE i.order_id = this.id LIMIT :lim;',
    }, '\n') .. '\n'
  )
  vim.cmd.edit(path)
  local bufnr = vim.api.nvim_get_current_buf()
  local function group(text)
    local line, col = find(bufnr, text)
    return vim.fn.synIDattr(vim.fn.synID(line + 1, col + 1, 1), 'name')
  end

  for text, expected in pairs({
    ['IMPORT'] = 'sqrlStatement',
    ['connectors.police_'] = 'sqrlPath',
    ['{{variant}}'] = 'sqrlTemplate',
    ['primary_key'] = 'sqrlHintName',
    ['id) */'] = 'sqrlHintArgument',
    ['Orders :='] = 'sqrlDefinition',
    [':='] = 'sqrlAssign',
    ['SELECT o.id'] = 'sqrlKeyword',
    ['COUNT'] = 'sqrlFunction',
    ["'open'"] = 'sqrlString',
    ['-- latest'] = 'sqrlLineComment',
    ['Orders.items'] = 'sqrlFunctionDefinition',
    ['INT)'] = 'sqrlType',
    ['this.id'] = 'sqrlThis',
    [':lim'] = 'sqrlParameter',
  }) do
    check(group(text) == expected, text .. ': ' .. group(text))
  end
  check(vim.bo[bufnr].commentstring == '-- %s', 'commentstring')
  vim.cmd('bwipeout!')
end)

local jar = vim.env.SQRL_CLI_JAR or (vim.fs.dirname(plugin_dir) .. '/sqrl-cli/target/sqrl-cli.jar')
if not vim.uv.fs_stat(jar) then
  print('skipped the tests of the language server, the jar of the CLI was not built: ' .. jar)
  os.exit(failures > 0 and 1 or 0)
end

require('sqrl').setup({ jar = jar })

-- The project of the tests of the IntelliJ plugin, whose main script has an unknown column
write(root .. '/shared/package.json', '{"version": "1", "script": {"config": {"interval": "10 sec"}}}\n')
write(
  root .. '/shared/orders.jsonl',
  table.concat({
    '{"id": 1, "customer_id": 10, "amount": 12.5, "updated_at": "2024-01-01T10:00:00.000Z"}',
    '{"id": 2, "customer_id": 11, "amount": 7.0, "updated_at": "2024-01-01T11:00:00.000Z"}',
  }, '\n') .. '\n'
)
write(
  root .. '/shared/sources.sqrl',
  table.concat({
    'CREATE TABLE Orders (',
    '  PRIMARY KEY (id, `updated_at`) NOT ENFORCED,',
    "  WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND",
    ') WITH (',
    "  'source.monitor-interval' = '{{interval}}'",
    ')',
    'LIKE `orders.jsonl`;',
  }, '\n') .. '\n'
)
write(
  root .. '/project/package.json',
  [[{
  "version": "1",
  "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
  "script": {
    "main": "main.sqrl",
    "include": {"shop": {"package": "../shared/package.json"}}
  }
}
]]
)
write(
  root .. '/project/main.sqrl',
  table.concat({
    'IMPORT shop.sources;',
    '',
    'OrderStream := SELECT * FROM sources.Orders;',
    'Orders := DISTINCT OrderStream ON id ORDER BY updated_at DESC;',
    'CustomerTotals := SELECT customer_id, SUM(amout) AS total FROM Orders GROUP BY customer_id;',
  }, '\n') .. '\n'
)
write(root .. '/web/main.sqrl', 'X := SELECT 1;\n')
write(root .. '/web/package.json', '{"dependencies": {"react": "19"}}\n')

vim.cmd.edit(root .. '/project/main.sqrl')
local main = vim.api.nvim_get_current_buf()

test('the server checks the script and reports its errors', function()
  local client = await('the server', function()
    return vim.lsp.get_clients({ bufnr = main, name = 'sqrl' })[1]
  end)
  check(client.root_dir == root .. '/project', 'root ' .. tostring(client.root_dir))

  local diagnostic = await('the error', function()
    return vim.tbl_filter(function(d)
      return d.message:find("Column 'amout' not found", 1, true) ~= nil
    end, vim.diagnostic.get(main))[1]
  end)
  local line, col = find(main, 'amout')
  check(diagnostic.lnum == line and diagnostic.col == col, vim.inspect(diagnostic))
end)

test('tables are documented, completed and colored', function()
  local line, col = find(main, 'Orders :=')
  local hover = request(main, 'textDocument/hover', line, col)
  check(hover and hover.contents.value:find('updated_at', 1, true), vim.inspect(hover))

  vim.api.nvim_buf_set_lines(main, -1, -1, false, { 'X := SELECT * FROM Ord' })
  local last = vim.api.nvim_buf_line_count(main) - 1
  local completion = request(main, 'textDocument/completion', last, #'X := SELECT * FROM Ord')
  local labels = vim.tbl_map(function(item)
    return item.label
  end, completion.items or completion)
  check(vim.list_contains(labels, 'OrderStream'), vim.inspect(labels))
  vim.api.nvim_buf_set_lines(main, -2, -1, false, {})

  local sum_line, sum_col = find(main, 'SUM(')
  local help = request(main, 'textDocument/signatureHelp', sum_line, sum_col + #'SUM(')
  check(help and help.signatures[1].label:find('^SUM%('), vim.inspect(help))

  local token_line = find(main, 'OrderStream :=')
  await('semantic tokens', function()
    return vim.tbl_filter(function(token)
      return token.opts.hl_group == '@lsp.type.class.sqrl'
    end, vim.inspect_pos(main, token_line, 0).semantic_tokens)[1]
  end)
end)

test('package files of the project use the server, those of npm do not', function()
  local client = vim.lsp.get_clients({ bufnr = main, name = 'sqrl' })[1]
  vim.cmd.edit(root .. '/project/package.json')
  local package_file = vim.api.nvim_get_current_buf()
  await('the package file', function()
    return vim.lsp.get_clients({ bufnr = package_file, name = 'sqrl' })[1]
  end)
  check(
    vim.lsp.get_clients({ bufnr = package_file, name = 'sqrl' })[1].id == client.id,
    'one server for the project'
  )

  vim.cmd.edit(root .. '/web/package.json')
  local npm = vim.api.nvim_get_current_buf()
  vim.wait(1000)
  check(#vim.lsp.get_clients({ bufnr = npm, name = 'sqrl' }) == 0, 'npm')
  vim.cmd.buffer(main)
end)

test('computed columns show their types, and tables are renamed', function()
  local line = find(main, 'amout')
  local text = vim.api.nvim_buf_get_lines(main, line, line + 1, false)[1]
  vim.api.nvim_buf_set_lines(main, line, line + 1, false, { (text:gsub('amout', 'amount')) })
  await('the type of `total`', function()
    return vim.tbl_filter(function(hint)
      local label = hint.inlay_hint.label
      return (type(label) == 'string' and label or '') == ': DOUBLE NOT NULL'
    end, vim.lsp.inlay_hint.get({ bufnr = main }))[1]
  end)

  local def_line, def_col = find(main, 'OrderStream :=')
  vim.api.nvim_win_set_cursor(0, { def_line + 1, def_col })
  vim.lsp.buf.rename('OrderEvents')
  await('the rename', function()
    local content = table.concat(vim.api.nvim_buf_get_lines(main, 0, -1, false), '\n')
    return content:find('OrderStream', 1, true) == nil and content:find('OrderEvents', 1, true)
  end)
end)

test('test tables have a code lens that runs the tests in a terminal', function()
  vim.api.nvim_buf_set_lines(
    main,
    -1,
    -1,
    false,
    { '/*+test */', 'TotalsTest := SELECT * FROM CustomerTotals;' }
  )
  local test_line = find(main, 'TotalsTest')
  local found = await('the code lens', function()
    return vim.tbl_filter(function(found)
      return found.lens.range.start.line == test_line
        and found.lens.command
        and found.lens.command.command == 'sqrl.runTests'
    end, vim.lsp.codelens.get({ bufnr = main }))[1]
  end)
  local package_file = vim.uri_to_fname(found.lens.command.arguments[1])
  check(package_file == root .. '/project/package.json', package_file)

  -- With a command that only shows what it runs, instead of the CLI
  local options = require('sqrl').options
  options.cmd = { 'echo', 'lsp' }
  vim.lsp.commands['sqrl.runTests'](found.lens.command, { client_id = found.client_id })
  options.cmd = nil
  local terminal = vim.api.nvim_get_current_buf()
  await('the tests in a terminal', function()
    local output = table.concat(vim.api.nvim_buf_get_lines(terminal, 0, -1, false), '\n')
    return output:find('test package.json', 1, true)
  end)
  vim.cmd('bwipeout!')
end)

for _, client in ipairs(vim.lsp.get_clients()) do
  client:stop()
end
vim.wait(10000, function()
  return #vim.lsp.get_clients() == 0
end)
vim.fn.delete(root, 'rf')
os.exit(failures > 0 and 1 or 0)
