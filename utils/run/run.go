package run

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
)

var Host = "LOCAL"

type StderrError string

func (self StderrError) Error() string {
	return string(self)
}

func Start(path string, args ...string) (result chan error) {
	return startAndLog("", path, args...)
}

func StartAndLog(logfile string, path string, args ...string) (result chan error) {
	return startAndLog(logfile, path, args...)
}

func startAndLog(logfile string, path string, args ...string) (result chan error) {
	result = make(chan error, 1)

	var file *os.File
	var err error
	if logfile != "" {
		if file, err = os.Create(logfile); err != nil {
			result <- err
			return
		}
	}

	fmt.Printf(" ( *** %v ) %v", Host, path)
	for _, bit := range args {
		fmt.Printf(" %#v", bit)
	}
	if logfile != "" {
		fmt.Printf(" > %#v\n", logfile)
	} else {
		fmt.Printf("\n")
	}

	cmd := exec.Command(path, args...)

	cmd.Stdin = os.Stdin
	if logfile != "" {
		cmd.Stdout, cmd.Stderr = file, file
	} else {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	if err = cmd.Start(); err != nil {
		result <- err
		return
	}

	go func() {
		if logfile != "" {
			defer file.Close()
		}
		result <- cmd.Wait()
	}()

	return
}

func RunAndReturn(path string, params ...string) (stdout, stderr string, err error) {
	fmt.Printf(" ( *** %v ) %v", Host, path)
	for _, bit := range params {
		fmt.Printf(" %#v", bit)
	}
	fmt.Println("")

	cmd := exec.Command(path, params...)
	o := new(bytes.Buffer)
	e := new(bytes.Buffer)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, o, e
	err = cmd.Run()
	stdout, stderr = o.String(), e.String()
	return
}

func RunSilent(path string, params ...string) (err error) {
	return run(true, path, params...)
}

func Run(path string, params ...string) (err error) {
	return run(false, path, params...)
}

func run(silent bool, path string, params ...string) (err error) {
	cmd := exec.Command(path, params...)
	buf := new(bytes.Buffer)
	if silent {
		cmd.Stderr = buf
	} else {
		cmd.Stderr = io.MultiWriter(buf, os.Stderr)
		cmd.Stdin, cmd.Stdout = os.Stdin, os.Stdout
		fmt.Printf(" ( *** %v ) %v", Host, path)
		for _, bit := range params {
			fmt.Printf(" %#v", bit)
		}
		fmt.Println("")
	}
	err = cmd.Run()
	if strings.TrimSpace(string(buf.Bytes())) != "" {
		err = StderrError(buf.String())
		return
	}
	if err != nil {
		return
	}
	return
}

type CommandSet struct {
	Commands    [][]string
	Names       []string
	TMux        bool
	SessionName string
	Detach      bool
}

func (self *CommandSet) InParallel() (err error) {
	if self.TMux {

		if self.SessionName == "" {
			self.SessionName = fmt.Sprintf("parallelize-%v", rand.Int63())
		}

		state := "newSession"
		windowCommands := []string{}
		for index, command := range self.Commands {
			params := []string{"tmux"}
			switch state {
			case "newSession":
				params = append(params, "new-session", "-s", self.SessionName)
				state = "splitWindow"
			case "splitWindow":
				params = append(params, "attach", "-t", self.SessionName, ";", "split-window")
				if index%4 == 3 {
					state = "newWindow"
				}
			case "newWindow":
				params = append(params, "attach", "-t", self.SessionName, ";", "rename-window", fmt.Sprint(windowCommands), ";", "new-window")
				windowCommands = []string{}
				state = "splitWindow"
			}
			if len(self.Names) > index {
				windowCommands = append(windowCommands, self.Names[index])
			}
			escapedCommand := []string{}
			for _, part := range command {
				escapedCommand = append(escapedCommand, fmt.Sprintf("%q", part))
			}
			params = append(params,
				strings.Join(escapedCommand, " "),
				";",
				"select-layout",
				"tiled",
				";",
				"detach-client")
			if err = Run(params[0], params[1:]...); err != nil {
				err = fmt.Errorf("Error running %#v: %v", params, err)
				return
			}
		}
		params := []string{"tmux", "attach", "-t", self.SessionName, ";", "rename-window", fmt.Sprint(windowCommands), ";", "select-layout", "tiled", ";", "detach-client"}
		if err = Run(params[0], params[1:]...); err != nil {
			err = fmt.Errorf("Error running %#v: %v", params, err)
			return
		}
		if !self.Detach {
			params := []string{"tmux", "attach", "-t", self.SessionName, ";", "select-window", "-t", "0"}
			if err = Run(params[0], params[1:]...); err != nil {
				err = fmt.Errorf("Error running %#v: %v", params, err)
				return
			}
		}
	} else {
		// Start all background processes, logging to files.
		var procs []chan error
		for _, command := range self.Commands {
			procs = append(procs, StartAndLog(fmt.Sprintf("%v.log", strings.Join(command, " ")), command[0], command[1:]...))
		}
		if !self.Detach {
			// Wait for all subprocesses to quit.
			for _, p := range procs {
				if err = <-p; err != nil {
					return
				}
			}
		}
	}
	return
}
