# swath completion

swath completion prints a shell completion script for bash, zsh or fish.

## Synopsis

```text
swath completion bash|zsh|fish
```

## Description

The script completes the commands, each command's options (long and short), the choices of an option that
takes one (`--format`, `--color`, `--metadata`), file and directory names where SOURCE or an option takes a
path, and the URL schemes `s3://`, `gs://` and `az://`. `swath resume` completes the options a resume may
give again, with the spellings that undo a recorded switch. The script is generated from the binary's own
option table, so it always matches the binary that printed it.

## Examples

Load completion in the current bash, or add the line to `~/.bashrc`:

```bash
source <(swath completion bash)
```

zsh, after `compinit` (`~/.zshrc`):

```bash
source <(swath completion zsh)
```

fish:

```bash
swath completion fish > ~/.config/fish/completions/swath.fish
```

## Exit status

0, or 2 for a missing or unknown shell.

## See also

[swath(1)](swath.md)
