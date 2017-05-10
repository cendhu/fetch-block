package main

import (
	"bytes"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	pbmsp "github.com/hyperledger/fabric/protos/msp"
	ab "github.com/hyperledger/fabric/protos/orderer"
	pb "github.com/hyperledger/fabric/protos/peer"
	utils "github.com/hyperledger/fabric/protos/utils"
	"github.com/spf13/viper"
)

var interestedEvents []*pb.Interest

//eventAdapter must implement GetInterestedEvents(), Recv() and Disconnect()
//which will be called by EventClient.
func (adapter *eventAdapter) GetInterestedEvents() ([]*pb.Interest, error) {
	return interestedEvents, nil
}

//We are intersted only in Event_Block events.
//Hence, we ignore the following events: Event_Register,
//Event_Unregister, Event_ChaincodeEvent, Event_Rejection
func (adapter *eventAdapter) Recv(msg *pb.Event) (bool, error) {
	if blockEvent, ok := msg.Event.(*pb.Event_Block); ok {
		adapter.block_channel <- blockEvent
		return true, nil
	}
	return false, fmt.Errorf("Not received block event. Event type is unknown: %v", msg)
}

func (adapter *eventAdapter) Disconnected(err error) {
	fmt.Print("Disconnected\n")
	os.Exit(1)
}

func startEventClient(peerEventAddress string) *eventAdapter {
	var eventClient *EventsClient
	var err error
	adapter := &eventAdapter{block_channel: make(chan *pb.Event_Block)}
	eventClient, _ = NewEventsClient(peerEventAddress, 10, adapter)
	if err = eventClient.Start(); err != nil {
		fmt.Printf("could not start chat %s\n", err)
		eventClient.Stop()
		return nil
	}
	return adapter
}

func prettyprint(b []byte) ([]byte, error) {
	var out bytes.Buffer
	err := json.Indent(&out, b, "", "  ")
	return out.Bytes(), err
}

func deserializeIdentity(serializedID []byte) (*x509.Certificate, error) {
	sId := &pbmsp.SerializedIdentity{}
	err := proto.Unmarshal(serializedID, sId)
	if err != nil {
		return nil, fmt.Errorf("Could not deserialize a SerializedIdentity, err %s", err)
	}

	bl, _ := pem.Decode(sId.IdBytes)
	if bl == nil {
		return nil, fmt.Errorf("Could not decode the PEM structure")
	}
	cert, err := x509.ParseCertificate(bl.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ParseCertificate failed %s", err)
	}

	return cert, nil
}

func copyChannelHeaderToLocalChannelHeader(localChannelHeader *ChannelHeader,
	chHeader *cb.ChannelHeader, chaincodeHeaderExtension *pb.ChaincodeHeaderExtension) {
	localChannelHeader.Type = chHeader.Type
	localChannelHeader.Version = chHeader.Version
	localChannelHeader.Timestamp = chHeader.Timestamp
	localChannelHeader.ChannelId = chHeader.ChannelId
	localChannelHeader.TxId = chHeader.TxId
	localChannelHeader.Epoch = chHeader.Epoch
	localChannelHeader.ChaincodeId = chaincodeHeaderExtension.ChaincodeId
}

func copyChaincodeSpecToLocalChaincodeSpec(localChaincodeSpec *ChaincodeSpec, chaincodeSpec *pb.ChaincodeSpec) {
	localChaincodeSpec.Type = chaincodeSpec.Type
	localChaincodeSpec.ChaincodeId = chaincodeSpec.ChaincodeId
	localChaincodeSpec.Timeout = chaincodeSpec.Timeout
	chaincodeInput := &ChaincodeInput{}
	for _, input := range chaincodeSpec.Input.Args {
		chaincodeInput.Args = append(chaincodeInput.Args, string(input))
	}
	localChaincodeSpec.Input = chaincodeInput
}

func copyEndorsementToLocalEndorsement(localTransaction *Transaction, allEndorsements []*pb.Endorsement) {
	for _, endorser := range allEndorsements {
		endorsement := &Endorsement{}
		endorserSignatureHeader := &cb.SignatureHeader{}
		if err := proto.Unmarshal(endorser.Endorser, endorserSignatureHeader); err != nil {
			fmt.Printf("Error unmarshaling endorser signature: %s\n", err)
		}

		endorsement.SignatureHeader = getSignatureHeaderFromBlockData(endorserSignatureHeader)
		endorsement.Signature = endorser.Signature
		localTransaction.Endorsements = append(localTransaction.Endorsements, endorsement)
	}
}

func getValueFromBlockMetadata(block *cb.Block, index cb.BlockMetadataIndex) []byte {
	valueMetadata := &cb.Metadata{}
	if err := proto.Unmarshal(block.Metadata.Metadata[index], valueMetadata); err != nil {
		return nil
	}
	if index == cb.BlockMetadataIndex_LAST_CONFIG {
		lastConfig := &cb.LastConfig{}
		if err := proto.Unmarshal(valueMetadata.Value, lastConfig); err != nil {
			return nil
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(lastConfig.Index))
		return b
	} else if index == cb.BlockMetadataIndex_ORDERER {
		kafkaMetadata := &ab.KafkaMetadata{}
		if err := proto.Unmarshal(valueMetadata.Value, kafkaMetadata); err != nil {
			return nil
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(kafkaMetadata.LastOffsetPersisted))
		return b
	}
	return valueMetadata.Value
}

func getSignatureHeaderFromBlockMetadata(block *cb.Block, index cb.BlockMetadataIndex) (*SignatureMetadata, error) {
	signatureMetadata := &cb.Metadata{}
	if err := proto.Unmarshal(block.Metadata.Metadata[index], signatureMetadata); err != nil {
		return nil, err
	}
	localSignatureHeader := &cb.SignatureHeader{}

	if len(signatureMetadata.Signatures) > 0 {
		if err := proto.Unmarshal(signatureMetadata.Signatures[0].SignatureHeader, localSignatureHeader); err != nil {
			return nil, err
		}

		localSignatureMetadata := &SignatureMetadata{}
		localSignatureMetadata.SignatureHeader = getSignatureHeaderFromBlockData(localSignatureHeader)
		localSignatureMetadata.Signature = signatureMetadata.Signatures[0].Signature

		return localSignatureMetadata, nil
	}
	return nil, nil
}

func getSignatureHeaderFromBlockData(header *cb.SignatureHeader) *SignatureHeader {
	signatureHeader := &SignatureHeader{}
	signatureHeader.Certificate, _ = deserializeIdentity(header.Creator)
	signatureHeader.Nonce = header.Nonce
	return signatureHeader

}

// This method add transaction validation information from block TransactionFilter struct
func addTransactionValidation(block *Block, tran *Transaction, txIdx int) error {
	if len(block.TransactionFilter) > txIdx {
		tran.ValidationCode = block.TransactionFilter[txIdx]
		tran.ValidationCodeName = pb.TxValidationCode_name[int32(tran.ValidationCode)]
		return nil
	}
	return fmt.Errorf("Invalid index or transaction filler. Index: %d", txIdx)
}

//var localMsp msp.MSP

func main() {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Fatol error when read config file: err %s\n", err)
	}
	peerEventAddress := viper.GetString("peer.event-address")
	if peerEventAddress == "" {
		fmt.Printf("Event address of the peer should not be empty\n")
	}
	mspDir := viper.GetString("msp.path")
	if mspDir == "" {
		fmt.Printf("MSP config path should not be empty\n")
	}
	mspID := viper.GetString("msp.localMspId")
	if mspID == "" {
		fmt.Printf("MSP ID should not be empty\n")
	}

	fmt.Printf("Peer Event Address: %s\n", peerEventAddress)
	fmt.Printf("Local MSP Directory: %s\n", mspDir)
	fmt.Printf("Local MSP ID: %s\n", mspID)

	err := mspmgmt.LoadLocalMsp(mspDir, nil, mspID)
	if err != nil {
		fmt.Printf("Fatal error when setting up MSP from directory: err %s\n", err)
	}

	event := &pb.Interest{EventType: pb.EventType_BLOCK}
	//for receiving blocks from specific channel, we can
	//pass channel id to pb.Interest as shown below:
	//event := &pb.Interest{EventType: EventType_BLOCK, ChainID: givenChannelID}
	//However, we are interested in receiving all blocks.
	interestedEvents = append(interestedEvents, event)
	fmt.Printf("Starting Client\n")
	adapter := startEventClient(peerEventAddress)
	if adapter == nil {
		fmt.Println("Error starting EventClient")
		return
	}
	for {
		fmt.Println("Listening for the event...\n")
		select {
		case blockEvent := <-adapter.block_channel:
			var block *cb.Block
			var localBlock Block
			block = blockEvent.Block
			localBlock.Header = block.Header
			localBlock.TransactionFilter = ledgerUtil.NewTxValidationFlags(len(block.Data.Data))

			// process block metadata before data
			localBlock.BlockCreatorSignature, _ = getSignatureHeaderFromBlockMetadata(block, cb.BlockMetadataIndex_SIGNATURES)
			lastConfigBlockNumber := &LastConfigMetadata{}
			lastConfigBlockNumber.LastConfigBlockNum = binary.LittleEndian.Uint64(getValueFromBlockMetadata(block, cb.BlockMetadataIndex_LAST_CONFIG))
			lastConfigBlockNumber.SignatureData, _ = getSignatureHeaderFromBlockMetadata(block, cb.BlockMetadataIndex_LAST_CONFIG)
			localBlock.LastConfigBlockNumber = lastConfigBlockNumber
			txBytes := getValueFromBlockMetadata(block, cb.BlockMetadataIndex_TRANSACTIONS_FILTER)
			for index, b := range txBytes {
				localBlock.TransactionFilter[index] = uint8(b)
			}
			ordererKafkaMetadata := &OrdererMetadata{}
			ordererKafkaMetadata.LastOffsetPersisted = binary.BigEndian.Uint64(getValueFromBlockMetadata(block, cb.BlockMetadataIndex_ORDERER))
			ordererKafkaMetadata.SignatureData, _ = getSignatureHeaderFromBlockMetadata(block, cb.BlockMetadataIndex_ORDERER)
			localBlock.OrdererKafkaMetadata = ordererKafkaMetadata

			for txIndex, data := range block.Data.Data {
				localTransaction := &Transaction{}
				//Get envelope which is stored as byte array in the data field.
				envelope, err := utils.GetEnvelopeFromBlock(data)
				if err != nil {
					fmt.Printf("Error getting envelope: %s\n", err)
				}
				localTransaction.Signature = envelope.Signature
				//Get payload from envelope struct which is stored as byte array.
				payload, err := utils.GetPayload(envelope)
				if err != nil {
					fmt.Printf("Error getting payload from envelope: %s\n", err)
				}
				chHeader, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
				if err != nil {
					fmt.Printf("Error unmarshaling channel header: %s\n", err)
				}
				headerExtension := &pb.ChaincodeHeaderExtension{}
				if err := proto.Unmarshal(chHeader.Extension, headerExtension); err != nil {
					fmt.Printf("Error unmarshaling chaincode header extension: %s\n", err)
				}
				localChannelHeader := &ChannelHeader{}
				copyChannelHeaderToLocalChannelHeader(localChannelHeader, chHeader, headerExtension)
				localTransaction.ChannelHeader = localChannelHeader
				localSignatureHeader := &cb.SignatureHeader{}
				if err := proto.Unmarshal(payload.Header.SignatureHeader, localSignatureHeader); err != nil {
					fmt.Printf("Error unmarshaling signature header: %s\n", err)
				}
				localTransaction.SignatureHeader = getSignatureHeaderFromBlockData(localSignatureHeader)
				//localTransaction.SignatureHeader.Nonce = localSignatureHeader.Nonce
				//localTransaction.SignatureHeader.Certificate, _ = deserializeIdentity(localSignatureHeader.Creator)
				transaction := &pb.Transaction{}
				if err := proto.Unmarshal(payload.Data, transaction); err != nil {
					fmt.Printf("Error unmarshaling transaction: %s\n", err)
				}
				chaincodeActionPayload, chaincodeAction, err := utils.GetPayloads(transaction.Actions[0])
				if err != nil {
					fmt.Printf("Error getting payloads from transaction actions: %s\n", err)
				}
				localSignatureHeader = &cb.SignatureHeader{}
				if err := proto.Unmarshal(transaction.Actions[0].Header, localSignatureHeader); err != nil {
					fmt.Printf("Error unmarshaling signature header: %s\n", err)
				}
				localTransaction.TxActionSignatureHeader = getSignatureHeaderFromBlockData(localSignatureHeader)
				//signatureHeader = &SignatureHeader{}
				//signatureHeader.Certificate, _ = deserializeIdentity(localSignatureHeader.Creator)
				//signatureHeader.Nonce = localSignatureHeader.Nonce
				//localTransaction.TxActionSignatureHeader = signatureHeader

				chaincodeProposalPayload := &pb.ChaincodeProposalPayload{}
				if err := proto.Unmarshal(chaincodeActionPayload.ChaincodeProposalPayload, chaincodeProposalPayload); err != nil {
					fmt.Printf("Error unmarshaling chaincode proposal payload: %s\n", err)
				}
				chaincodeInvocationSpec := &pb.ChaincodeInvocationSpec{}
				if err := proto.Unmarshal(chaincodeProposalPayload.Input, chaincodeInvocationSpec); err != nil {
					fmt.Printf("Error unmarshaling chaincode invocationSpec: %s\n", err)
				}
				localChaincodeSpec := &ChaincodeSpec{}
				copyChaincodeSpecToLocalChaincodeSpec(localChaincodeSpec, chaincodeInvocationSpec.ChaincodeSpec)
				localTransaction.ChaincodeSpec = localChaincodeSpec
				copyEndorsementToLocalEndorsement(localTransaction, chaincodeActionPayload.Action.Endorsements)
				proposalResponsePayload := &pb.ProposalResponsePayload{}
				if err := proto.Unmarshal(chaincodeActionPayload.Action.ProposalResponsePayload, proposalResponsePayload); err != nil {
					fmt.Printf("Error unmarshaling proposal response payload: %s\n", err)
				}
				localTransaction.ProposalHash = proposalResponsePayload.ProposalHash
				localTransaction.Response = chaincodeAction.Response
				events := &pb.ChaincodeEvent{}
				if err := proto.Unmarshal(chaincodeAction.Events, events); err != nil {
					fmt.Printf("Error unmarshaling chaincode action events:%s\n", err)
				}
				localTransaction.Events = events

				txReadWriteSet := &rwset.TxReadWriteSet{}
				if err := proto.Unmarshal(chaincodeAction.Results, txReadWriteSet); err != nil {
					fmt.Printf("Error unmarshaling chaincode action results: %s\n", err)
				}

				if len(chaincodeAction.Results) != 0 {
					for _, nsRwset := range txReadWriteSet.NsRwset {
						nsReadWriteSet := &NsReadWriteSet{}
						kvRWSet := &kvrwset.KVRWSet{}
						nsReadWriteSet.Namespace = nsRwset.Namespace
						if err := proto.Unmarshal(nsRwset.Rwset, kvRWSet); err != nil {
							fmt.Printf("Error unmarshaling tx read write set: %s\n", err)
						}
						localkvRWSet := &KVRWSet{}
						localkvRWSet.Reads = kvRWSet.Reads

						var writes []*KVWrite
						for _, write := range kvRWSet.Writes {
							localWrite := &KVWrite{}
							localWrite.Key = write.Key
							localWrite.IsDelete = write.IsDelete
							localWrite.Value = string(write.Value)
							writes = append(writes, localWrite)
						}
						localkvRWSet.Writes = writes
						nsReadWriteSet.KVRWSet = localkvRWSet
						localTransaction.NsRwset = append(localTransaction.NsRwset, nsReadWriteSet)
					}
				}

				// add the transaction validation a
				addTransactionValidation(&localBlock, localTransaction, txIndex)

				//append the transaction
				localBlock.Transactions = append(localBlock.Transactions, localTransaction)
			}
			blockJSON, _ := json.Marshal(localBlock)
			blockJSONString, _ := prettyprint(blockJSON)
			fmt.Printf("Received Block [%d] from ChannelId [%s]", localBlock.Header.Number, localBlock.Transactions[0].ChannelHeader.ChannelId)
			fileName := localBlock.Transactions[0].ChannelHeader.ChannelId + "_blk#" + strconv.FormatUint(localBlock.Header.Number, 10) + ".json"
			f, _ := os.Create("./" + fileName)
			_, _ = f.WriteString(string(blockJSONString))
		}
	}
}
