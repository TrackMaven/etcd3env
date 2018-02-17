package etcdenv

import (
	"errors"
	"os"
	"strings"
	"time"
	"context"


	"github.com/cenkalti/backoff"
	"github.com/coreos/etcd/clientv3"
	"github.com/upfluence/goutils/log"
)

type Context struct {
	Namespaces        []string
	Runner            *Runner
	ExitChan          chan bool
	ShutdownBehaviour string
	WatchedPrefix	  string
	Watches           map[string]clientv3.WatchChan
	CurrentEnv        map[string]string
	maxRetry          int
	etcdClient        *clientv3.Client
}

func NewClient(namespaces []string, endpoints, command []string,
	shutdownBehaviour string, watchedPrefix string) (*Context, error) {

	if shutdownBehaviour != "keepalive" && shutdownBehaviour != "restart" &&
		shutdownBehaviour != "exit" {
		return nil,
			errors.New(
				"Choose a correct shutdown behaviour : keepalive | exit | restart",
			)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Errorf("Could not connect to server: %s", err.Error())
		return nil, err
	}

	return &Context{
		Namespaces:        namespaces,
		Runner:            NewRunner(command),
		etcdClient:        client,
		ShutdownBehaviour: shutdownBehaviour,
		ExitChan:          make(chan bool),
		WatchedPrefix:     watchedPrefix,
		Watches:		   make(map[string]clientv3.WatchChan),
		CurrentEnv:        make(map[string]string),
		maxRetry:          3,
	}, nil
}

func (ctx *Context) escapeNamespace(key string) string {
	for _, namespace := range ctx.Namespaces {
		if strings.HasPrefix(key, namespace) {
			key = strings.TrimPrefix(key, namespace)
			break
		}
	}

	return strings.TrimPrefix(key, "/")
}

func (ctx *Context) fetchEtcdNamespaceVariables(namespace string, currentRetry int, b *backoff.ExponentialBackOff) (map[string]string, error) {
	vars := make(map[string]string)

	con, cancel := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
	resp, err := ctx.etcdClient.Get(con, namespace, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		log.Errorf("etcd fetching error: %s", err.Error())
		return vars, err
	}
	for _, ev := range resp.Kvs {
		vars[strings.Replace(string(ev.Key), namespace, "", -1)] = string(ev.Value)
	}

	return vars, nil
}

func (ctx *Context) fetchEtcdVariables() map[string]string {
	result := make(map[string]string)

	b := backoff.NewExponentialBackOff()

	for _, namespace := range ctx.Namespaces {
		b.Reset()
		response, err := ctx.fetchEtcdNamespaceVariables(namespace, 0, b)
		if err != nil {
			log.Errorf("etcd fetching error: %s", err.Error())
			return nil
		}
		for key, value := range response {
			if _, ok := result[key]; !ok {
				result[key] = value
			}
		}
	}

	return result
}

func (ctx *Context) Run() {
	ctx.CurrentEnv = ctx.fetchEtcdVariables()
	ctx.Runner.Start(ctx.CurrentEnv)

	responseChan := make(chan *clientv3.WatchResponse)
	processExitChan := make(chan int)

	if len(ctx.WatchedPrefix) > 0 {
		con, cancel := context.WithCancel(context.Background())
		cancelRoutine := make(chan bool)
		defer close(cancelRoutine)

		go func() {
			select {
			case <-ctx.ExitChan:
				cancel()
			case <-cancelRoutine:
				return
			}
		}()

		rch := ctx.Watches[ctx.WatchedPrefix]
		if rch == nil {
			rch = ctx.etcdClient.Watch(con, ctx.WatchedPrefix, clientv3.WithPrefix())
			ctx.Watches[ctx.WatchedPrefix] = rch
		}

		for wresp := range rch {
			for _, ev := range wresp.Events {
				log.Debug("Key updated %s", string(ev.Kv.Key))
				// Only return if we have a key prefix we care about.
				// This is not an exact match on the key so there is a chance
				// we will still pickup on false positives. The net win here
				// is reducing the scope of keys that can trigger updates.
				for _, k := range ctx.CurrentEnv {
					if strings.HasPrefix(string(ev.Kv.Key), k) {
						responseChan <- &wresp
					}
				}
			}
		}
	}

	// for _, namespace := range ctx.Namespaces {
	// 	go func(namespace string) {
	// 		var t time.Duration
	// 		b := backoff.NewExponentialBackOff()
	// 		b.Reset()

	// 		for {
	// 			resp, err := ctx.etcdClient.Watch(namespace, 0, true, nil, ctx.ExitChan)

	// 			if err != nil {
	// 				log.Errorf("etcd fetching error: %s", err.Error())

	// 				if e, ok := err.(*rpctypes.EtcdError); ok && e.ErrorCode == clientv3.ErrCodeEtcdNotReachable {
	// 					t = b.NextBackOff()
	// 					log.Noticef("Can't join the etcd server, wait %v", t)
	// 					time.Sleep(t)
	// 				}

	// 				if t == backoff.Stop {
	// 					return
	// 				} else {
	// 					continue
	// 				}
	// 			}

	// 			log.Infof("%s key changed", resp.Node.Key)

	// 			if ctx.shouldRestart(ctx.escapeNamespace(resp.Node.Key), resp.Node.Value) {
	// 				responseChan <- resp
	// 			}
	// 		}
	// 	}(namespace)
	// }

	go ctx.Runner.WatchProcess(processExitChan)

	for {
		select {
		case <-responseChan:
			log.Notice("Environment changed, restarting child process..")
			ctx.CurrentEnv = ctx.fetchEtcdVariables()
			ctx.Runner.Restart(ctx.CurrentEnv)
			log.Notice("Process restarted")
		case <-ctx.ExitChan:
			log.Notice("Asking the runner to stop")
			ctx.Runner.Stop()
			log.Notice("Runner stopped")
		case status := <-processExitChan:
			log.Noticef("Child process exited with status %d", status)
			if ctx.ShutdownBehaviour == "exit" {
				ctx.ExitChan <- true
				os.Exit(status)
			} else if ctx.ShutdownBehaviour == "restart" {
				ctx.CurrentEnv = ctx.fetchEtcdVariables()
				ctx.Runner.Restart(ctx.CurrentEnv)
				go ctx.Runner.WatchProcess(processExitChan)
				log.Notice("Process restarted")
			}
		}
	}
}

func containsString(keys []string, item string) bool {
	for _, elt := range keys {
		if elt == item {
			return true
		}
	}

	return false
}
