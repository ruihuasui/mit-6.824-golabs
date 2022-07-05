# MIT 6.824 Distributed System Learning
This repo contains code written through learning golang and couse [MIT6.824 SP2020](http://nil.csail.mit.edu/6.824/2020/index.html).

There are some changes made to the original source code, in order to use go version _1.18.3_ for labs.

## Run Labs
### [Lab1 MapReduce](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html)
1. Go Inside `src/main/`
2. Start the master server
```bash
$ go run mrmaster.go pg-*.txt
```
3. Start the workers
```bash
$ go build -buildmode=plugin ./mrapps/wc.go && go run mrworker.go wc.so
```
