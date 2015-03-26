package ssh

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/soundtrackyourbrand/ssh"
	utilsRun "github.com/soundtrackyourbrand/utils/run"
)

func ParseCreds(user string, b []byte) (result Creds, err error) {
	k, err := ssh.ParsePrivateKey(b)
	if err != nil {
		return
	}
	result = Creds{
		keys: []ssh.Signer{k},
		user: user,
	}
	return
}

type Creds struct {
	keys []ssh.Signer
	user string
}

func (self Creds) Key(i int) (key ssh.PublicKey, err error) {
	if i < len(self.keys) {
		key = self.keys[i].PublicKey()
	}
	return
}

func (self Creds) Sign(i int, rand io.Reader, data []byte) (sig []byte, err error) {
	return self.keys[i].Sign(rand, data)
}

func Rsync(user string, key []byte, addr, src, dst string, excludes ...string) (err error) {
	creds, err := ParseCreds(user, key)
	if err != nil {
		return
	}
	if err = Run(creds, fmt.Sprintf("%v:22", addr), fmt.Sprintf("mkdir -p %#v", dst)); err != nil {
		return
	}
	tmpdir, err := ioutil.TempDir("", "utilsSshRsync")
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			os.RemoveAll(tmpdir)
		}
	}()
	outfilename := filepath.Join(tmpdir, "identity")
	outfile, err := os.Create(outfilename)
	if err != nil {
		return
	}
	if _, err = outfile.Write(key); err != nil {
		return
	}
	if err = outfile.Close(); err != nil {
		return
	}
	if err = os.Chmod(outfilename, 0400); err != nil {
		return
	}
	params := []string{"rsync", fmt.Sprintf("%#v", fmt.Sprintf("--rsh=ssh -i %#v -o \"StrictHostKeyChecking no\"", outfilename)), "--progress", "-avzc", "-C", "--delete"}
	for _, excl := range excludes {
		params = append(params, "--exclude", excl)
	}
	params = append(params, fmt.Sprintf("%v", src), fmt.Sprintf("%v@%v:%v", user, addr, dst))
	if err = utilsRun.Run("sh", "-c", strings.Join(params, " ")+" 2>&1"); err != nil {
		return
	}
	return
}

func TarCopy(creds Creds, addr, src, dst string, excludes ...string) (err error) {
	sess, err := New(creds, addr)
	if err != nil {
		return
	}

	params := []string{}
	for _, exclude := range excludes {
		params = append(params, "--exclude", exclude)
	}
	params = append(params, "-c", "-z", "-C", filepath.Dir(src), filepath.Base(src))
	tar := exec.Command("tar", params...)

	pipein, pipeout := io.Pipe()
	sess.Stdin, sess.Stdout, sess.Stderr = pipein, os.Stdout, os.Stderr
	tar.Stdin, tar.Stdout, tar.Stderr = os.Stdin, pipeout, os.Stderr

	remoteDone := make(chan struct{})

	go func() {
		cmd := fmt.Sprintf("mkdir -p %#v && tar -x -v -z -C %#v", dst, dst)
		fmt.Printf(" *** ( %v ) %#v\n", addr, cmd)
		if err := sess.Run(cmd); err != nil {
			panic(err)
		}
		close(remoteDone)
	}()

	fmt.Printf(" ( *** %v ) %v", utilsRun.Host, "tar")
	for _, bit := range params {
		fmt.Printf(" %#v", bit)
	}
	fmt.Println("")
	if err = tar.Run(); err != nil {
		return
	}
	if err = pipeout.Close(); err != nil {
		return
	}

	<-remoteDone

	return
}

func Run(creds Creds, addr, cmd string) (err error) {
	sess, err := New(creds, addr)
	if err != nil {
		return
	}

	in, out := io.Pipe()
	sess.Stdin, sess.Stdout, sess.Stderr = in, os.Stdout, os.Stderr

	remoteDone := make(chan struct{})

	go func() {
		fmt.Printf(" *** ( %v ) %#v\n", addr, cmd)
		if err = sess.Run(cmd); err != nil {
			return
		}
		close(remoteDone)
	}()
	if err = out.Close(); err != nil {
		return
	}
	<-remoteDone
	return
}

func New(creds Creds, addr string) (result *ssh.Session, err error) {
	sshConn, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		User: creds.user,
		Auth: []ssh.ClientAuth{
			ssh.ClientAuthKeyring(creds),
		},
	})

	if err != nil {
		return
	}

	result, err = sshConn.NewSession()
	return
}
