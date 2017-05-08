/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package producer

import (

	"github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// SendProducerBlockEvent sends block event to clients
func SendProducerBlockEvent(block *common.Block) error {
	logger.Debugf("Entry")
	defer logger.Debugf("Exit")

	logger.Infof("Sending event for block number [%d]", block.Header.Number)

	return Send(CreateBlockEvent(block))
}

//CreateBlockEvent creates a Event from a Block
func CreateBlockEvent(te *common.Block) *pb.Event {
	return &pb.Event{Event: &pb.Event_Block{Block: te}}
}

//CreateChaincodeEvent creates a Event from a ChaincodeEvent
func CreateChaincodeEvent(te *pb.ChaincodeEvent) *pb.Event {
	return &pb.Event{Event: &pb.Event_ChaincodeEvent{ChaincodeEvent: te}}
}

//CreateRejectionEvent creates an Event from TxResults
func CreateRejectionEvent(tx *pb.Transaction, errorMsg string) *pb.Event {
	return &pb.Event{Event: &pb.Event_Rejection{Rejection: &pb.Rejection{Tx: tx, ErrorMsg: errorMsg}}}
}
