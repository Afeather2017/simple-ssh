# Simple ssh

This repo is a simple ssh, which have no security stuff, and allows only command interaction.
So some command will failed, say, vim.

The built binary is only server side, for client side, nc is enough.

## How to use

Compile it and you got a binary, which open port 9999.

Use nc to connect the binary, first line you should input the shell you want. you can use cmd.exe, pwsh.exe, bash, zsh, etc. And on remote the shell running, you will see the prompt the shell show.
