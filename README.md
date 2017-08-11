# coordinate

coordinate is a distributed KV with strong consistence and It provides a simple REST API for a key-value store cluster. 

## Getting Started

### Build

go build . -o coordinate

### run single one node

First start a single-member cluster of coordinate:

```sh
./coordinate --id 1 --cluster http://127.0.0.1:12379 --port 12380
```

Each coordinate maintains a RAFT instance and a Key-Value server. The process's list of comma separated peers (--cluster), its raft ID index into the peer list (--id), and http key-value server port (--port) are passed through the command line.

Then, we could store a value ("hello") to a key ("my-key"):

```
curl -L http://127.0.0.1:12380/my-key -XPUT -d hello
```

Finally, retrieve the stored key:

```
curl -L http://127.0.0.1:12380/my-key
```

### Running a local cluster

For example, suppose we have a 3-node cluster that was started with the commands:
```sh
./coordinate --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
./coordinate --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
./coordinate --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
```

A fourth node with ID 4 can be added by issuing a POST:
```sh
curl -L http://127.0.0.1:12380/4 -XPOST -d http://127.0.0.1:42379
```

Then the new node can be started as the others were, using the --join option:
```sh
./coordinate --id 4 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379,http://127.0.0.1:42379 --port 42380 --join
```

The new node should join the cluster and be able to service key/value requests.

We can remove a node using a DELETE request:
```sh
curl -L http://127.0.0.1:12380/3 -XDELETE
```

Node 3 should shut itself down once the cluster has processed this request.

## Design

The coordinate consists of three components: a raft-backed key-value store, a REST API server, and a raft consensus server based on etcd's raft implementation.

The raft-backed key-value store is a key-value map that holds all committed key-values.
The store bridges communication between the raft server and the REST server.
Key-value updates are issued through the store to the raft server.
The store updates its map once raft reports the updates are committed.

The REST server exposes the current raft consensus by accessing the raft-backed key-value store.
A GET command looks up a key in the store and returns the value, if any.
A key-value PUT command issues an update proposal to the store.

The raft server participates in consensus with its cluster peers.
When the REST server submits a proposal, the raft server transmits the proposal to its peers.
When raft reaches a consensus, the server publishes all committed updates over a commit channel.
For coordinate, this commit channel is consumed by the key-value store.

