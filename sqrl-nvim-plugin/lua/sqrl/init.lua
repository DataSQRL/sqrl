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
--- Support for DataSQRL scripts: starts the SQRL language server of the DataSQRL CLI, `sqrl lsp`, for
--- scripts, package files and GraphQL schemas. See `:help sqrl`.
local command = require('sqrl.command')
local project = require('sqrl.project')

local M = {}

--- @class sqrl.Options
--- @field cmd? string[] The command that starts the server, instead of `jar` or Docker
--- @field jar? string The jar of the CLI, `sqrl-cli.jar`, to start the server with Java
--- @field java? string The Java executable for `jar`, by default of `JAVA_HOME` or on the `PATH`
--- @field docker string The Docker executable
--- @field docker_image string The Docker image of the CLI
--- @field settings table The settings of the server: `packageFiles`, `checkDelayMillis` and `fullCompileOnSave`
--- @field inlay_hints boolean Whether to show the types of computed columns
--- @field completion boolean Whether to complete with the built-in completion of Neovim as you type
--- @field codelens boolean Whether to show the code lenses that run the tests above test tables

--- @type sqrl.Options
M.defaults = {
  docker = 'docker',
  docker_image = 'datasqrl/cmd:latest',
  settings = {},
  inlay_hints = true,
  completion = false,
  codelens = true,
}

--- The command of the code lenses above test tables, with the URIs of the package files
M.RUN_TESTS_COMMAND = 'sqrl.runTests'

--- The options of the last `setup()`
--- @type sqrl.Options?
M.options = nil

--- The configuration of the server for `vim.lsp.config()`.
--- @param options sqrl.Options
--- @return vim.lsp.Config
function M.config(options)
  return {
    cmd = function(dispatchers, config)
      return vim.lsp.rpc.start(
        command.build(options, config.root_dir),
        dispatchers,
        { cwd = command.workdir() }
      )
    end,
    filetypes = { 'sqrl', 'json', 'graphql' },
    root_dir = project.root_dir,
    init_options = next(options.settings) and options.settings or nil,
  }
end

--- Configures and enables the server.
--- @param opts? sqrl.Options
function M.setup(opts)
  M.options = vim.tbl_deep_extend('force', vim.deepcopy(M.defaults), opts or {})
  -- Also when the plugin is added to the runtimepath after its plugin/ files were loaded
  vim.filetype.add({ extension = { sqrl = 'sqrl' } })
  vim.lsp.config('sqrl', M.config(M.options))
  vim.lsp.enable('sqrl')

  vim.api.nvim_create_autocmd('LspAttach', {
    group = vim.api.nvim_create_augroup('sqrl', { clear = true }),
    callback = function(args)
      local client = vim.lsp.get_client_by_id(args.data.client_id)
      if not client or client.name ~= 'sqrl' then
        return
      end
      if M.options.inlay_hints then
        vim.lsp.inlay_hint.enable(true, { bufnr = args.buf })
      end
      if M.options.completion then
        vim.lsp.completion.enable(true, client.id, args.buf, { autotrigger = true })
      end
      if M.options.codelens then
        if vim.lsp.codelens.enable then
          vim.lsp.codelens.enable(true, { bufnr = args.buf })
        else
          vim.lsp.codelens.refresh({ bufnr = args.buf })
        end
      end
    end,
  })

  -- The code lenses run the tests in a terminal, see |vim.lsp.codelens.run()|
  vim.lsp.commands[M.RUN_TESTS_COMMAND] = function(command, ctx)
    local client = vim.lsp.get_client_by_id(ctx.client_id)
    local package_files = vim.tbl_map(vim.uri_to_fname, command.arguments or {})
    M.run_tests(package_files, client and client.root_dir)
  end
end

--- Runs the tests of a project in a terminal, `sqrl test` with its package files.
--- @param package_files string[] the package files of the project
--- @param root? string the root directory of the server
function M.run_tests(package_files, root)
  if #package_files == 0 then
    vim.notify('The script is not part of a project with a package file', vim.log.levels.WARN)
    return
  end
  local test = command.test(M.options, root, package_files)
  vim.cmd('botright new')
  vim.fn.jobstart(test.cmd, { term = true, cwd = test.cwd })
end

return M
