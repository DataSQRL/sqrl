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
--- `:checkhealth sqrl`: whether the language server can be started, and the servers that run.
local command = require('sqrl.command')

local M = {}

--- The major version of a Java executable, e.g. 17, or nil if it does not run.
local function java_version(java)
  local ok, result = pcall(function()
    return vim.system({ java, '-version' }, { text = true }):wait(10000)
  end)
  if not ok or result.code ~= 0 then
    return nil
  end
  local version = (result.stderr or ''):match('version "(%d+)')
  return version and tonumber(version) or nil
end

local function check_command(options)
  if options.cmd then
    vim.health.info('Starts the server with `' .. table.concat(options.cmd, ' ') .. '`')
    if vim.fn.executable(options.cmd[1]) ~= 1 then
      vim.health.error('`' .. options.cmd[1] .. '` is not executable')
    end
  elseif options.jar then
    local jar = vim.fs.normalize(options.jar)
    if vim.uv.fs_stat(jar) then
      vim.health.ok('Starts the server from the jar ' .. jar)
    else
      vim.health.error('The jar of the CLI does not exist: ' .. jar)
    end
    local java = command.java(options)
    local version = java_version(java)
    if not version then
      vim.health.error('Java does not run: ' .. java, 'Set `java` or `JAVA_HOME`')
    elseif version < 17 then
      vim.health.error('The CLI needs Java 17 or later, but ' .. java .. ' is Java ' .. version)
    else
      vim.health.ok('Java ' .. version .. ': ' .. java)
    end
  else
    vim.health.info('Starts the server in the Docker image ' .. options.docker_image)
    if vim.fn.executable(options.docker) ~= 1 then
      vim.health.error('Docker was not found', 'Install Docker, or set `jar` to the jar of the CLI')
    else
      vim.health.ok('Docker: ' .. vim.fn.exepath(options.docker))
    end
  end
end

function M.check()
  vim.health.start('sqrl')
  if vim.fn.has('nvim-0.11') ~= 1 then
    vim.health.error('Neovim 0.11 or later is needed')
    return
  end
  local options = require('sqrl').options
  if not options then
    vim.health.warn("The server is not configured", "Call `require('sqrl').setup()`")
    return
  end
  check_command(options)
  local clients = vim.lsp.get_clients({ name = 'sqrl' })
  if #clients == 0 then
    vim.health.info('No server runs, it starts when a script is opened')
  end
  for _, client in ipairs(clients) do
    local buffers = vim.tbl_count(client.attached_buffers)
    vim.health.ok(
      ('Server %d runs for %s, with %d open files'):format(client.id, client.root_dir or '?', buffers)
    )
  end
end

return M
