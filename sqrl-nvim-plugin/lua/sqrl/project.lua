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
--- Finds the DataSQRL projects of files, and decides which files the language server is started for:
--- scripts, DataSQRL package files and the GraphQL files of projects.
local M = {}

--- Sections of a DataSQRL package file, which tell it apart from other JSON files. `engines` is
--- left out, since the package.json of npm has it as well.
local DATASQRL_SECTIONS = { 'script', 'enabled-engines', 'connectors', 'compiler', 'test-runner' }

--- Sections of the package.json of npm
local NPM_SECTIONS = { 'dependencies', 'devDependencies', 'peerDependencies', 'scripts', 'workspaces' }

local MAX_PACKAGE_FILE = 1024 * 1024

local function has_section(text, sections)
  for _, section in ipairs(sections) do
    if text:find('"' .. vim.pesc(section) .. '"%s*:') then
      return true
    end
  end
  return false
end

local function read(path)
  local stat = vim.uv.fs_stat(path)
  if not stat or stat.size > MAX_PACKAGE_FILE then
    return nil
  end
  local file = io.open(path, 'r')
  if not file then
    return nil
  end
  local text = file:read('*a')
  file:close()
  return text
end

local function has_scripts(dir)
  for name, type in vim.fs.dir(dir) do
    if type == 'file' and name:match('%.sqrl$') then
      return true
    end
  end
  return false
end

--- Whether a file name is that of a package file, e.g. `package.json` or `orders_test_package.json`.
--- @param name string
--- @return boolean
function M.is_package_name(name)
  return name:lower():find('package', 1, true) ~= nil and name:match('%.json$') ~= nil
end

--- Whether a file is a DataSQRL package file: one with sections of DataSQRL, or one next to scripts
--- without sections of npm, e.g. a new one.
--- @param path string
--- @param text? string the content of the file, if it is not read from disk
--- @return boolean
function M.is_package_file(path, text)
  if not M.is_package_name(vim.fs.basename(path)) then
    return false
  end
  text = text or read(path)
  if not text then
    return false
  end
  if has_section(text, DATASQRL_SECTIONS) then
    return true
  end
  return not has_section(text, NPM_SECTIONS) and has_scripts(vim.fs.dirname(path))
end

--- The directory of the DataSQRL project of a file: the nearest directory with a package file, from
--- the directory of the file upwards.
--- @param path string
--- @return string?
function M.project_dir(path)
  local found = vim.fs.find(function(name, dir)
    return M.is_package_name(name) and M.is_package_file(vim.fs.joinpath(dir, name))
  end, { path = vim.fs.dirname(path), upward = true, type = 'file', limit = 1 })
  return found[1] and vim.fs.dirname(found[1]) or nil
end

--- Decides whether the language server is started for a buffer, and with which root. One server
--- serves all projects of a repository, since projects include each other's files, and the Docker
--- image of the CLI only sees the root.
--- @param bufnr integer
--- @param on_dir fun(root_dir?: string)
function M.root_dir(bufnr, on_dir)
  local path = vim.api.nvim_buf_get_name(bufnr)
  if path == '' then
    return
  end
  local filetype = vim.bo[bufnr].filetype
  local project
  if filetype == 'sqrl' then
    -- The server warns about scripts that are not part of a project
    project = M.project_dir(path) or vim.fs.dirname(path)
  elseif filetype == 'json' then
    local text = table.concat(vim.api.nvim_buf_get_lines(bufnr, 0, -1, false), '\n')
    if M.is_package_file(path, text) then
      project = vim.fs.dirname(path)
    end
  elseif filetype == 'graphql' then
    project = M.project_dir(path)
  end
  if project then
    on_dir(vim.fs.root(project, '.git') or project)
  end
end

return M
