To compile the fetch-block tool, run the following command

```$ cd fabric/fetch-block```

```$ go build```

In order to compile this tool successfully, make sure that fabric/ is placed in ```$ $GOPATH/src/github.com/hyperledger/```

After successful compilation, we can run the tool using $ ./fetch-block (by default, it listens on 0.0.0.0:7053). 

You can also give custom fabric event port using $ ./fetch-block -address 192.168.100.1:1053

When you start executing transaction on blockchain, the blocks are printed in a JSON format.

Refer to https://blockchain-fabric.blogspot.in/ for block structure.
