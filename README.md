Keep the src directory on any place in fabric/ and compile using $ go build 

Then, we can run the problem using $ ./fetch-block (by default, it listens on 0.0.0.0:7053). 

You can also give custom fabric event port using $ ./fetch-block -address 192.168.100.1:1053

When you start executing transaction on blockchain, the block is printed in a JSON format.

Refer to blockchain-fabric.blogspot.com for block structure.
