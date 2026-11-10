// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
)

// completionShells are the shells swath completion writes a script for.
var completionShells = []string{"bash", "zsh", "fish"}

// completionHelp is swath completion's help text.
var completionHelp = commandHelp{usage: []string{"completion bash|zsh|fish"},
	about:    "Print the completion script of a shell on stdout: the commands, each command's options and their choices, and file names, directories and URL schemes where SOURCE or an option takes one. The script is generated from this build's option table, so a new option needs no change to it. Load it from the shell's startup file.",
	examples: []string{"source <(swath completion bash)", "source <(swath completion zsh)", "swath completion fish > ~/.config/fish/completions/swath.fish"}}

// compKind is how a flag's value completes.
type compKind string

const (
	kindSwitch compKind = ""      // takes no value
	kindEnum   compKind = "enum"  // one of choices
	kindFile   compKind = "file"  // a file name
	kindDir    compKind = "dir"   // a directory name
	kindValue  compKind = "value" // a value nothing completes
)

// compFlag is a flag as completion offers it.
type compFlag struct {
	long, short, usage, metavar string
	kind                        compKind
	choices                     []string
}

// names are the flag as typed: --long and -s.
func (f compFlag) names() []string {
	n := []string{"--" + f.long}
	if f.short != "" {
		n = append(n, "-"+f.short)
	}
	return n
}

// words are an enum flag's choices, space separated.
func (f compFlag) words() string { return strings.Join(f.choices, " ") }

// What follows a context's flags.
const (
	operandNone   = ""
	operandSource = "source"
	operandDir    = "dir"
)

// compContext is a command (or the bare root, key "") with the flags it
// takes and what its operand is.
type compContext struct {
	key     string // as typed: "list", "diag count"; "" for swath itself
	flags   []compFlag
	operand string
}

// valued are the flags of c that take a value.
func (c compContext) valued() []compFlag {
	var v []compFlag
	for _, f := range c.flags {
		if f.kind != kindSwitch {
			v = append(v, f)
		}
	}
	return v
}

// compFlagOf is fl as completion offers it, by its metavar: a|b|c is an
// enum, PATH and FILE name files, DIR a directory, none a switch.
func compFlagOf(fl *pflag.Flag) compFlag {
	d := source.Described(fl)
	c := compFlag{long: fl.Name, short: fl.Shorthand, usage: fl.Usage, metavar: d.Metavar, kind: kindValue}
	switch {
	case d.Metavar == "":
		c.kind = kindSwitch
	case strings.Contains(d.Metavar, "|"):
		c.kind, c.choices = kindEnum, strings.Split(d.Metavar, "|")
	case d.Metavar == "PATH" || d.Metavar == "FILE":
		c.kind = kindFile
	case d.Metavar == "DIR":
		c.kind = kindDir
	}
	return c
}

var (
	helpFlag    = compFlag{long: "help", short: "h", usage: "print help"}
	versionFlag = compFlag{long: "version", short: "V", usage: "print the version"}
)

// completionContexts reads the flag table: the root's flags, those each
// list command's help shows, swath resume's and swath show's.
func completionContexts() []compContext {
	var all []*pflag.Flag
	newFlags(&options{}, true).VisitAll(func(fl *pflag.Flag) {
		if !fl.Hidden {
			all = append(all, fl)
		}
	})
	collect := func(keep func(*pflag.Flag) bool) []compFlag {
		var out []compFlag
		for _, fl := range all {
			if keep(fl) {
				out = append(out, compFlagOf(fl))
			}
		}
		return append(out, helpFlag)
	}
	root := collect(func(fl *pflag.Flag) bool { return slices.Contains(globalFlags, fl.Name) })
	ctxs := []compContext{{flags: append(root, versionFlag)}}
	for _, cmd := range []command{{}, {count: true, du: true}, {diag: true}, {diag: true, count: true}} {
		ctxs = append(ctxs, compContext{key: cmd.name(), operand: operandSource,
			flags: collect(func(fl *pflag.Flag) bool { return shown(fl, cmd) })})
	}
	return append(ctxs,
		compContext{key: "resume", operand: operandDir, flags: collect(func(fl *pflag.Flag) bool { return mayChange(fl.Name) })},
		compContext{key: "show", operand: operandDir, flags: []compFlag{{long: "journal", usage: "dump the journal's header and records as JSON lines"}, helpFlag}})
}

// compCommand is a command word with its one-line description.
type compCommand struct{ word, about string }

// completionCommands are the root help's commands, one per word.
func completionCommands() []compCommand {
	var out []compCommand
	for _, c := range rootCommands {
		for _, w := range strings.Split(c[0], ", ") {
			out = append(out, compCommand{w, c[1]})
		}
	}
	return out
}

func commandWordsOf(commands []compCommand) string {
	var w []string
	for _, c := range commands {
		w = append(w, c.word)
	}
	return strings.Join(w, " ")
}

// completionSchemes are the URL prefixes SOURCE completes to.
func completionSchemes() []string {
	var s []string
	for _, f := range registry.Factories() {
		if f.Scheme != "" {
			s = append(s, f.Scheme+"://")
		}
	}
	sort.Strings(s)
	return s
}

// completion runs swath completion SHELL.
func completion(args []string, stdout io.Writer) error {
	f := pflag.NewFlagSet("completion", pflag.ContinueOnError)
	f.SetOutput(io.Discard)
	p, err := parseFlags(f, "completion", args, nil)
	if errors.Is(err, pflag.ErrHelp) {
		var b strings.Builder
		completionHelp.write(&b, styleFor(stdout, "auto", p.longHelp))
		fmt.Fprint(stdout, b.String())
		return err
	}
	switch {
	case err != nil:
		return err
	case len(p.operands) == 0:
		return seeHelp{usageError{errors.New("missing SHELL (bash, zsh or fish)")}, "completion"}
	case len(p.operands) > 1:
		return seeHelp{usageError{fmt.Errorf("unexpected argument %q", registry.Redact(p.operands[1]))}, "completion"}
	}
	return writeCompletion(stdout, p.operands[0])
}

// writeCompletion writes shell's script.
func writeCompletion(w io.Writer, shell string) error {
	ctxs := completionContexts()
	var b strings.Builder
	switch shell {
	case "bash":
		bashScript(&b, ctxs)
	case "zsh":
		zshScript(&b, ctxs)
	case "fish":
		fishScript(&b, ctxs)
	default:
		return seeHelp{usageError{fmt.Errorf("unknown shell %q (bash, zsh or fish)", registry.Redact(shell))}, "completion"}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// operandKeys are the quoted keys of the contexts whose operand is kind,
// as a case pattern.
func operandKeys(ctxs []compContext, kind string) string {
	var k []string
	for _, c := range ctxs {
		if c.operand == kind {
			k = append(k, "'"+c.key+"'")
		}
	}
	return strings.Join(k, "|")
}

// ---- bash

func bashScript(b *strings.Builder, ctxs []compContext) {
	commands := commandWordsOf(completionCommands())
	b.WriteString("# bash completion for swath; generated by swath completion bash\n\n")

	b.WriteString("_swath_kind() {\n    case $1 in\n")
	seen := map[string]bool{}
	for _, c := range ctxs {
		for _, f := range c.valued() {
			if seen[f.long] {
				continue
			}
			seen[f.long] = true
			val := string(f.kind)
			if f.kind == kindEnum {
				val += " " + f.words()
			}
			fmt.Fprintf(b, "    %s) echo '%s';;\n", strings.Join(f.names(), "|"), val)
		}
	}
	b.WriteString("    esac\n}\n\n")

	b.WriteString("_swath_flags() {\n    case $1 in\n")
	for _, c := range ctxs {
		var names []string
		for _, f := range c.flags {
			names = append(names, f.names()...)
		}
		fmt.Fprintf(b, "    '%s') echo '%s';;\n", c.key, strings.Join(names, " "))
	}
	b.WriteString("    esac\n}\n\n")

	b.WriteString("_swath() {\n    local cur prev cmd sub i w flag kind\n")
	b.WriteString("    cur=${COMP_WORDS[COMP_CWORD]}\n    prev=\n    ((COMP_CWORD > 0)) && prev=${COMP_WORDS[COMP_CWORD-1]}\n")
	b.WriteString("    COMPREPLY=()\n\n")
	b.WriteString("    # --flag=value arrives as three words: --flag, = and the value.\n")
	b.WriteString("    if [[ $cur == = ]]; then\n        flag=$prev cur=\n")
	b.WriteString("    elif [[ $prev == = ]]; then\n        flag=${COMP_WORDS[COMP_CWORD-2]}\n")
	b.WriteString("    elif [[ $prev == -* ]]; then\n        flag=$prev\n    fi\n\n")

	b.WriteString("    for ((i = 1; i < COMP_CWORD; i++)); do\n        w=${COMP_WORDS[i]}\n        case $w in\n")
	if v := ctxs[0].valued(); len(v) > 0 {
		var names []string
		for _, f := range v {
			names = append(names, f.names()...)
		}
		fmt.Fprintf(b, "        %s)\n            ((i++))\n            [[ ${COMP_WORDS[i]} == = ]] && ((i++))\n            continue;;\n", strings.Join(names, "|"))
	}
	b.WriteString("        -*) continue;;\n        esac\n")
	b.WriteString("        if [[ -z $cmd ]]; then\n            cmd=$w\n            [[ $cmd == diag ]] || break\n        else\n            sub=$w\n            break\n        fi\n    done\n")
	b.WriteString("    [[ $cmd == ls ]] && cmd=list\n    [[ $cmd == diag && -n $sub ]] && cmd=\"diag $sub\"\n\n")

	b.WriteString("    if [[ -n $flag ]]; then\n        kind=$(_swath_kind \"$flag\")\n        case ${kind%% *} in\n")
	b.WriteString("        enum) COMPREPLY=($(compgen -W \"${kind#enum }\" -- \"$cur\"));;\n")
	b.WriteString("        file) COMPREPLY=($(compgen -f -- \"$cur\")); compopt -o filenames 2>/dev/null;;\n")
	b.WriteString("        dir) COMPREPLY=($(compgen -d -- \"$cur\")); compopt -o filenames 2>/dev/null;;\n")
	b.WriteString("        esac\n        [[ -n $kind ]] && return\n    fi\n\n")

	b.WriteString("    if [[ $cur == -* ]]; then\n        COMPREPLY=($(compgen -W \"$(_swath_flags \"$cmd\")\" -- \"$cur\"))\n        return\n    fi\n\n")

	b.WriteString("    case $cmd in\n")
	fmt.Fprintf(b, "    '') COMPREPLY=($(compgen -W '%s' -- \"$cur\"));;\n", commands)
	fmt.Fprintf(b, "    help) COMPREPLY=($(compgen -W '%s' -- \"$cur\"));;\n", commands)
	b.WriteString("    diag) COMPREPLY=($(compgen -W 'list count' -- \"$cur\"));;\n")
	fmt.Fprintf(b, "    completion) COMPREPLY=($(compgen -W '%s' -- \"$cur\"));;\n", strings.Join(completionShells, " "))
	fmt.Fprintf(b, "    %s)\n        COMPREPLY=($(compgen -W '%s' -- \"$cur\") $(compgen -d -- \"$cur\"))\n", operandKeys(ctxs, operandSource), strings.Join(completionSchemes(), " "))
	b.WriteString("        compopt -o filenames 2>/dev/null\n        [[ ${#COMPREPLY[@]} -eq 1 && ${COMPREPLY[0]} == *:// ]] && compopt -o nospace 2>/dev/null;;\n")
	fmt.Fprintf(b, "    %s) COMPREPLY=($(compgen -d -- \"$cur\")); compopt -o filenames 2>/dev/null;;\n", operandKeys(ctxs, operandDir))
	b.WriteString("    esac\n}\n\ncomplete -F _swath swath\n")
}

// ---- zsh

// zshQuote is s as one single-quoted zsh word.
func zshQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// zshDesc is a description inside an _arguments [...].
func zshDesc(s string) string {
	return strings.NewReplacer(`\`, `\\`, `[`, `\[`, `]`, `\]`).Replace(s)
}

// zshSpec is f as an _arguments specification.
func zshSpec(f compFlag) string {
	spec := "[" + zshDesc(f.usage) + "]"
	switch f.kind {
	case kindEnum:
		spec += ":value:(" + f.words() + ")"
	case kindFile:
		spec += ":" + f.metavar + ":_files"
	case kindDir:
		spec += ":" + f.metavar + ":_files -/"
	case kindValue:
		spec += ":" + f.metavar + ": "
	}
	long, short := "--"+f.long, "-"+f.short
	if f.kind != kindSwitch {
		long, short = long+"=", short+"+"
	}
	if f.short == "" {
		return zshQuote(long + spec)
	}
	return zshQuote("(-"+f.short+" --"+f.long+")") + "{" + short + "," + long + "}" + zshQuote(spec)
}

func zshFuncName(key string) string {
	return "_swath_" + strings.ReplaceAll(key, " ", "_")
}

func zshScript(b *strings.Builder, ctxs []compContext) {
	commands := completionCommands()
	b.WriteString("#compdef swath\n# zsh completion for swath; generated by swath completion zsh\n\n")
	fmt.Fprintf(b, "_swath_source() {\n    _alternative 'schemes:scheme:compadd -S \"\" -- %s' 'directories:directory:_files -/'\n}\n\n", strings.Join(completionSchemes(), " "))
	for _, c := range ctxs[1:] {
		fmt.Fprintf(b, "%s() {\n    _arguments \\\n", zshFuncName(c.key))
		for _, f := range c.flags {
			fmt.Fprintf(b, "        %s \\\n", zshSpec(f))
		}
		if c.operand == operandSource {
			b.WriteString("        '1:source:_swath_source'\n")
		} else {
			b.WriteString("        '1:directory:_files -/'\n")
		}
		b.WriteString("}\n\n")
	}
	b.WriteString("_swath_diag() {\n    local context state state_descr line\n    typeset -A opt_args\n    _arguments -C \\\n        '(-h --help)'{-h,--help}'[print help]' \\\n        '1:command:(list count)' \\\n        '*::arg:->args'\n    case $line[1] in\n    list) _swath_diag_list;;\n    count) _swath_diag_count;;\n    esac\n}\n\n")
	b.WriteString("_swath() {\n    local context state state_descr line\n    typeset -A opt_args\n    _arguments -C \\\n")
	for _, f := range ctxs[0].flags {
		fmt.Fprintf(b, "        %s \\\n", zshSpec(f))
	}
	b.WriteString("        '1:command:->command' \\\n        '*::arg:->args'\n    case $state in\n    command)\n        local -a commands\n        commands=(\n")
	for _, c := range commands {
		fmt.Fprintf(b, "            %s\n", zshQuote(strings.ReplaceAll(c.word, ":", `\:`)+":"+strings.ReplaceAll(c.about, ":", `\:`)))
	}
	b.WriteString("        )\n        _describe -t commands command commands;;\n    args)\n        case $line[1] in\n")
	b.WriteString("        list|ls) _swath_list;;\n        du) _swath_du;;\n        resume) _swath_resume;;\n        show) _swath_show;;\n        diag) _swath_diag;;\n")
	fmt.Fprintf(b, "        help) _arguments '1:command:(%s)';;\n", commandWordsOf(commands))
	fmt.Fprintf(b, "        completion) _arguments '1:shell:(%s)';;\n", strings.Join(completionShells, " "))
	b.WriteString("        esac;;\n    esac\n}\n\n")
	b.WriteString("if [ \"$funcstack[1]\" = _swath ]; then\n    _swath \"$@\"\nelse\n    compdef _swath swath\nfi\n")
}

// ---- fish

// fishQuote is s as one single-quoted fish word.
func fishQuote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// fishKey is a context key as one argument of __swath_cmd.
func fishKey(key string) string { return `"` + key + `"` }

func fishScript(b *strings.Builder, ctxs []compContext) {
	commands := completionCommands()
	b.WriteString("# fish completion for swath; generated by swath completion fish\n\n")
	b.WriteString("function __swath_key\n    set -l tokens (commandline -opc)\n    set -e tokens[1]\n    set -l words\n    set -l skip 0\n    for t in $tokens\n        if test $skip -eq 1\n            set skip 0\n            continue\n        end\n        switch $t\n")
	if v := ctxs[0].valued(); len(v) > 0 {
		var names []string
		for _, f := range v {
			names = append(names, f.names()...)
		}
		fmt.Fprintf(b, "            case %s\n                set skip 1\n", strings.Join(names, " "))
	}
	b.WriteString("            case '-*'\n            case '*'\n                set -a words $t\n        end\n    end\n")
	b.WriteString("    switch \"$words[1]\"\n        case ls\n            echo list\n        case diag\n            string join ' ' -- $words[1] $words[2]\n        case '*'\n            echo $words[1]\n    end\nend\n\n")
	b.WriteString("function __swath_cmd\n    set -l key (__swath_key)\n    contains -- \"$key\" $argv\nend\n\n")
	b.WriteString("complete -c swath -f\n\n")

	for _, c := range commands {
		fmt.Fprintf(b, "complete -c swath -n '__swath_cmd \"\"' -a %s -d %s\n", fishQuote(c.word), fishQuote(c.about))
	}
	b.WriteString("complete -c swath -n '__swath_cmd \"help\"' -a " + fishQuote(commandWordsOf(commands)) + "\n")
	b.WriteString("complete -c swath -n '__swath_cmd \"diag\"' -a 'list count'\n")
	b.WriteString("complete -c swath -n '__swath_cmd \"completion\"' -a " + fishQuote(strings.Join(completionShells, " ")) + "\n\n")

	// A flag is declared once, for the commands that take it.
	var order []string
	byFlag := map[string]compFlag{}
	takers := map[string][]string{}
	for _, c := range ctxs {
		for _, f := range c.flags {
			if _, ok := byFlag[f.long]; !ok {
				order = append(order, f.long)
				byFlag[f.long] = f
			}
			takers[f.long] = append(takers[f.long], fishKey(c.key))
		}
	}
	for _, long := range order {
		f := byFlag[long]
		fmt.Fprintf(b, "complete -c swath -n '__swath_cmd %s' -l %s", strings.Join(takers[long], " "), f.long)
		if f.short != "" {
			fmt.Fprintf(b, " -s %s", f.short)
		}
		switch f.kind {
		case kindEnum:
			fmt.Fprintf(b, " -x -a %s", fishQuote(f.words()))
		case kindFile:
			b.WriteString(" -r -F")
		case kindDir:
			b.WriteString(" -r -a '(__fish_complete_directories)'")
		case kindValue:
			b.WriteString(" -x")
		}
		fmt.Fprintf(b, " -d %s\n", fishQuote(f.usage))
	}

	b.WriteString("\n")
	for _, c := range ctxs {
		if c.operand == operandSource {
			fmt.Fprintf(b, "complete -c swath -n '__swath_cmd %s' -a %s\n", fishKey(c.key), fishQuote(strings.Join(completionSchemes(), " ")))
		}
		if c.operand != operandNone {
			fmt.Fprintf(b, "complete -c swath -n '__swath_cmd %s' -a '(__fish_complete_directories)'\n", fishKey(c.key))
		}
	}
}
