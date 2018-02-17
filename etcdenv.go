package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"./etcdenv"
	"github.com/upfluence/goutils/log"
)

const currentVersion = "0.4.0"

var (
	flagset = flag.NewFlagSet("etcd3env", flag.ExitOnError)
	flags   = struct {
		Version           bool
		ShutdownBehaviour string
		Server            string
		Namespace         string
		WatchedPrefix     string
	}{}
)

func usage() {
	fmt.Fprintf(os.Stderr, `
  NAME
  etcd3env - use your etcd keys as environment variables

  USAGE
  etcd3env [options] <command>

  OPTIONS
  `)
	flagset.PrintDefaults()
}

func init() {
	flagset.BoolVar(&flags.Version, "version", false, "Print the version and exit")
	flagset.BoolVar(&flags.Version, "v", false, "Print the version and exit")

	flagset.StringVar(&flags.ShutdownBehaviour, "b", "exit", "Behaviour when the process stop [exit|keepalive|restart]")
	flagset.StringVar(&flags.ShutdownBehaviour, "shutdown-behaviour", "exit", "Behaviour when the process stop [exit|keepalive|restart]")

	flagset.StringVar(&flags.Server, "server", "http://127.0.0.1:4001", "Location of the etcd server")
	flagset.StringVar(&flags.Server, "s", "http://127.0.0.1:4001", "Location of the etcd server")

	flagset.StringVar(&flags.Namespace, "namespace", "/environments/production", "etcd directory where the environment variables are fetched")
	flagset.StringVar(&flags.Namespace, "n", "/environments/production", "etcd directory where the environment variables are fetched")

	flagset.StringVar(&flags.WatchedPrefix, "watched", "", "environment variables prefix to watch")
	flagset.StringVar(&flags.WatchedPrefix, "w", "", "environment variables prefix to watch")
}

func main() {
	flagset.Parse(os.Args[1:])
	flagset.Usage = usage

	if len(os.Args) < 2 {
		flagset.Usage()
		os.Exit(0)
	}

	if flags.Version {
		fmt.Printf("etcd3env v%s", currentVersion)
		os.Exit(0)
	}

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	cli, err := etcdenv.NewClient(
		strings.Split(flags.Namespace, ","),
		[]string{flags.Server},
		flagset.Args(),
		flags.ShutdownBehaviour,
		flags.WatchedPrefix,
	)

	if err != nil {
		log.Fatalf(err.Error())
		os.Exit(1)
	}

	go cli.Run()

	select {
	case sig := <-signalChan:
		log.Noticef("Received signal %s", sig)
		cli.ExitChan <- true
	case <-cli.ExitChan:
		log.Infof("Catching ExitChan, doing nothing")
	}
}
