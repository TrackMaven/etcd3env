package etcdenv

import (
    "errors"
    "os"
    "strings"
    "time"
    "context"

    "github.com/coreos/etcd/clientv3"
    "github.com/upfluence/goutils/log"
)

type Context struct {
    Namespaces        []string
    Runner            *Runner
    ExitChan          chan bool
    ShutdownBehaviour string
    CurrentEnv        map[string]string
    etcdClient        *clientv3.Client
}

func NewClient(namespaces []string, endpoints, command []string,
    shutdownBehaviour string) (*Context, error) {

    if shutdownBehaviour != "keepalive" && shutdownBehaviour != "restart" &&
        shutdownBehaviour != "exit" {
        return nil,
            errors.New(
                "Choose a correct shutdown behaviour : keepalive | exit | restart",
            )
    }

    maxRetries := 10

    for i := 0; i < maxRetries; i++ {
        client, err := clientv3.New(clientv3.Config{
            Endpoints:   endpoints,
            DialTimeout: 5 * time.Second,
        })

        if err != nil {
            log.Errorf("Could not connect to server: %s", err.Error())
            time.Sleep(5 * time.Second)
        } else {
            log.Noticef("Connected to server: %s", endpoints)
            return &Context{
                Namespaces:        namespaces,
                Runner:            NewRunner(command),
                etcdClient:        client,
                ShutdownBehaviour: shutdownBehaviour,
                ExitChan:          make(chan bool),
                CurrentEnv:        make(map[string]string),
            }, nil
        }
    }

    return nil, errors.New(
        "Could not connect to server. Exiting.",
    )


}

func (ctx *Context) fetchEtcdNamespaceVariables(namespace string) (map[string]string) {
    vars := make(map[string]string)

    con, cancel := context.WithTimeout(context.Background(), time.Duration(5) * time.Second)
    resp, err := ctx.etcdClient.Get(con, namespace, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
    cancel()
    if err != nil {
        log.Fatalf("Failed to fetch envs: %s", err.Error())
        os.Exit(1)
        return vars
    }
    for _, ev := range resp.Kvs {
        vars[strings.Replace(string(ev.Key), namespace, "", -1)] = string(ev.Value)
    }

    return vars
}

func (ctx *Context) fetchEtcdVariables() map[string]string {
    result := make(map[string]string)

    for _, namespace := range ctx.Namespaces {
        response := ctx.fetchEtcdNamespaceVariables(namespace)

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

    processExitChan := make(chan int)

    go ctx.Runner.WatchProcess(processExitChan)

    for {
        select {
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
