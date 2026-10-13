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
--- The commands that start the SQRL language server, `sqrl lsp`.
local M = {}

--- The JVM options that the CLI needs, as in `SQRL_JVM_TOOL_OPTS` of its Docker image
M.JVM_OPTIONS = {
  '--add-exports=java.base/sun.net.util=ALL-UNNAMED',
  '--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED',
  '--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED',
  '--add-opens=java.base/java.lang=ALL-UNNAMED',
  '--add-opens=java.base/java.net=ALL-UNNAMED',
  '--add-opens=java.base/java.io=ALL-UNNAMED',
  '--add-opens=java.base/java.nio=ALL-UNNAMED',
  '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
  '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
  '--add-opens=java.base/java.text=ALL-UNNAMED',
  '--add-opens=java.base/java.time=ALL-UNNAMED',
  '--add-opens=java.base/java.util=ALL-UNNAMED',
  '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
  '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED',
  '--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED',
}

--- Where the Docker image of the CLI starts, which it requires to exist
M.WORKSPACE = '/workspace'

--- The Java executable: the configured one, of `JAVA_HOME`, or on the `PATH`.
--- @param options sqrl.Options
--- @return string
function M.java(options)
  if options.java then
    return options.java
  end
  local home = vim.env.JAVA_HOME
  if home and home ~= '' then
    return vim.fs.joinpath(home, 'bin', 'java')
  end
  return 'java'
end

--- The working directory of the server, in the cache of Neovim: files that checks create at relative
--- paths, e.g. a local Iceberg warehouse, end up there instead of in a project.
--- @return string
function M.workdir()
  local dir = vim.fs.joinpath(vim.fn.stdpath('cache'), 'sqrl-lsp')
  vim.fn.mkdir(dir, 'p')
  return dir
end

--- The command that starts the server: the configured one, from the jar of the CLI, or else in the
--- Docker image of the CLI. The editor refers to files by their paths, so Docker mounts the root at
--- its own path. The image starts in its workspace, which it requires, so that is an empty
--- temporary directory: files that checks create at relative paths stay out of the project.
--- @param options sqrl.Options
--- @param root_dir string
--- @return string[]
function M.build(options, root_dir)
  if options.cmd then
    return options.cmd
  end
  if options.jar then
    local command = { M.java(options) }
    vim.list_extend(command, M.JVM_OPTIONS)
    vim.list_extend(command, { '-jar', vim.fs.normalize(options.jar), 'lsp' })
    return command
  end
  return {
    options.docker,
    'run',
    '--interactive',
    '--rm',
    '--tmpfs',
    M.WORKSPACE,
    '--volume',
    root_dir .. ':' .. root_dir,
    options.docker_image,
    'lsp',
  }
end

--- Where the Docker image of the CLI has the jar of the CLI
M.IMAGE_JAR = '/opt/sqrl/sqrl-cli.jar'

--- The command that runs the tests of a project, `sqrl test`: the configured command, or else the
--- Docker image of the CLI, which has the services that tests need, e.g. Kafka and Postgres, with the
--- root at its workspace, so that included packages are there too. With `jar`, that jar runs in the
--- image.
--- @param options sqrl.Options
--- @param root? string the root directory of the server, which contains the project
--- @param package_files string[] the package files of the project
--- @return { cmd: string[], cwd: string }
function M.test(options, root, package_files)
  local project_dir = vim.fs.dirname(package_files[1])
  local function relative(dir)
    return vim.tbl_map(function(file)
      return vim.fs.relpath(dir, file) or file
    end, package_files)
  end
  if options.cmd then
    -- The command that starts the server with `lsp` runs the tests with `test`
    local cmd = vim.deepcopy(options.cmd)
    if cmd[#cmd] == 'lsp' then
      table.remove(cmd)
    end
    table.insert(cmd, 'test')
    vim.list_extend(cmd, relative(project_dir))
    return { cmd = cmd, cwd = project_dir }
  end
  local base = root and vim.fs.relpath(root, project_dir) and root or project_dir
  local cmd = { options.docker, 'run', '--rm', '--interactive', '--tty' }
  vim.list_extend(cmd, { '--volume', base .. ':' .. M.WORKSPACE })
  if options.jar then
    vim.list_extend(cmd, { '--volume', vim.fs.normalize(options.jar) .. ':' .. M.IMAGE_JAR })
  end
  vim.list_extend(cmd, { options.docker_image, 'test' })
  vim.list_extend(cmd, relative(base))
  return { cmd = cmd, cwd = base }
end

return M
