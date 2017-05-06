Clone the fetch-block repo to ```$ $GOPATH/src/github.com/hyperledger/fabrici/``` using ```git clone https://github.com/cendhu/fetch-block``

To compile the fetch-block tool, run the following command

```$ cd fabric/fetch-block```

```$ go build```

If compilation fails, make sure that fabric/ is placed in ```$ $GOPATH/src/github.com/hyperledger/fabric```

After successful compilation, we can run the tool using ```$ ./fetch-block``` 

Default configurations are given in ```config.yaml```. Update config file as per the need. Make sure that the peer and fetch-block use the same MSP configuration.

When we start executing transaction on blockchain, each block is stroed in a file (with name ChannelId_blk#.json) as JSON format.  

Refer to https://blockchain-fabric.blogspot.in/ for understanding the block structure.
