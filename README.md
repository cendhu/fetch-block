To compile the fetch-block tool, run the following command

```$ cd fabric/fetch-block```

```$ go build```

If compilation fails, make sure that fabric/ is placed in ```$ $GOPATH/src/github.com/hyperledger/```

After successful compilation, we can run the tool using ```$ ./fetch-block``` (by default, it listens on 0.0.0.0:7053 and use local MSP directory, i.e., ```./msp```, for signing event registration message with the peer, and use DEFAULT MSP ID). Make sure that the peer and fetch-block use the same MSP identity.

We can also give custom fabric event port and msp directory using ```$ ./fetch-block -address 192.168.100.1:1053 -mspDir "/path/to/msp" -mspID "Org0MSP"``` 

When we start executing transaction on blockchain, each block is stroed in a file (with name ChannelId_blk#.json) as JSON format.  

Refer to https://blockchain-fabric.blogspot.in/ for understanding the block structure.
