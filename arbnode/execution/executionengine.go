package execution

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/gorilla/websocket"
	"github.com/offchainlabs/nitro/arbos"
	"github.com/offchainlabs/nitro/arbos/arbosState"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbos/l1pricing"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/staker"
	"github.com/offchainlabs/nitro/util/sharedmetrics"
	"github.com/offchainlabs/nitro/util/stopwaiter"
	"github.com/pkg/errors"
)

type TransactionStreamerInterface interface {
	WriteMessageFromSequencer(pos arbutil.MessageIndex, msgWithMeta arbostypes.MessageWithMetadata) error
	ExpectChosenSequencer() error
	FetchBatch(batchNum uint64) ([]byte, error)
}

type ExecutionEngine struct {
	stopwaiter.StopWaiter

	bc        *core.BlockChain
	validator *staker.BlockValidator
	streamer  TransactionStreamerInterface

	resequenceChan    chan []*arbostypes.MessageWithMetadata
	createBlocksMutex sync.Mutex

	newBlockNotifier chan struct{}
	latestBlockMutex sync.Mutex
	latestBlock      *types.Block

	nextScheduledVersionCheck time.Time // protected by the createBlocksMutex

	reorgSequencing bool

	// hack
	headerchs      map[int]chan []*types.Log
	receiptschs    map[int]chan types.Receipts
	receiptsShmChs chan types.Receipts
	globalCountH   int
	globalCountR   int
	mutex          *sync.RWMutex
}

func NewExecutionEngine(bc *core.BlockChain) (*ExecutionEngine, error) {
	return &ExecutionEngine{
		bc:               bc,
		resequenceChan:   make(chan []*arbostypes.MessageWithMetadata),
		newBlockNotifier: make(chan struct{}, 1),
		headerchs:        make(map[int]chan []*types.Log, 100),
		receiptschs:      make(map[int]chan types.Receipts, 100),
		receiptsShmChs:   make(chan types.Receipts, 100),
		globalCountH:     0,
		globalCountR:     0,
		mutex:            &sync.RWMutex{},
	}, nil
}

func (s *ExecutionEngine) SetBlockValidator(validator *staker.BlockValidator) {
	if s.Started() {
		panic("trying to set block validator after start")
	}
	if s.validator != nil {
		panic("trying to set block validator when already set")
	}
	s.validator = validator
}

func (s *ExecutionEngine) EnableReorgSequencing() {
	if s.Started() {
		panic("trying to enable reorg sequencing after start")
	}
	if s.reorgSequencing {
		panic("trying to enable reorg sequencing when already set")
	}
	s.reorgSequencing = true
}

func (s *ExecutionEngine) SetTransactionStreamer(streamer TransactionStreamerInterface) {
	if s.Started() {
		panic("trying to set reorg sequencing policy after start")
	}
	if s.streamer != nil {
		panic("trying to set reorg sequencing policy when already set")
	}
	s.streamer = streamer
}

func (s *ExecutionEngine) Reorg(count arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadata, oldMessages []*arbostypes.MessageWithMetadata) error {
	s.createBlocksMutex.Lock()
	resequencing := false
	defer func() {
		// if we are resequencing old messages - don't release the lock
		// lock will be relesed by thread listening to resequenceChan
		if !resequencing {
			s.createBlocksMutex.Unlock()
		}
	}()
	blockNum, err := s.MessageCountToBlockNumber(count)
	if err != nil {
		return err
	}
	// We can safely cast blockNum to a uint64 as it comes from MessageCountToBlockNumber
	targetBlock := s.bc.GetBlockByNumber(uint64(blockNum))
	if targetBlock == nil {
		log.Warn("reorg target block not found", "block", blockNum)
		return nil
	}
	if s.validator != nil {
		err = s.validator.ReorgToBlock(targetBlock.NumberU64(), targetBlock.Hash())
		if err != nil {
			return err
		}
	}

	err = s.bc.ReorgToOldBlock(targetBlock)
	if err != nil {
		return err
	}
	for i := range newMessages {
		err := s.digestMessageWithBlockMutex(count+arbutil.MessageIndex(i), &newMessages[i])
		if err != nil {
			return err
		}
	}
	if len(oldMessages) > 0 {
		s.resequenceChan <- oldMessages
		resequencing = true
	}
	return nil
}

func (s *ExecutionEngine) getCurrentHeader() (*types.Header, error) {
	currentBlock := s.bc.CurrentBlock()
	if currentBlock == nil {
		return nil, errors.New("failed to get current block")
	}
	return currentBlock.Header(), nil
}

func (s *ExecutionEngine) HeadMessageNumber() (arbutil.MessageIndex, error) {
	currentHeader, err := s.getCurrentHeader()
	if err != nil {
		return 0, err
	}
	msgCount, err := s.BlockNumberToMessageCount(currentHeader.Number.Uint64())
	if err != nil {
		return 0, err
	}
	return msgCount - 1, err
}

func (s *ExecutionEngine) HeadMessageNumberSync(t *testing.T) (arbutil.MessageIndex, error) {
	s.createBlocksMutex.Lock()
	defer s.createBlocksMutex.Unlock()
	return s.HeadMessageNumber()
}

func (s *ExecutionEngine) NextDelayedMessageNumber() (uint64, error) {
	currentHeader, err := s.getCurrentHeader()
	if err != nil {
		return 0, err
	}
	return currentHeader.Nonce.Uint64(), nil
}

func messageFromTxes(header *arbostypes.L1IncomingMessageHeader, txes types.Transactions, txErrors []error) (*arbostypes.L1IncomingMessage, error) {
	var l2Message []byte
	if len(txes) == 1 && txErrors[0] == nil {
		txBytes, err := txes[0].MarshalBinary()
		if err != nil {
			return nil, err
		}
		l2Message = append(l2Message, arbos.L2MessageKind_SignedTx)
		l2Message = append(l2Message, txBytes...)
	} else {
		l2Message = append(l2Message, arbos.L2MessageKind_Batch)
		sizeBuf := make([]byte, 8)
		for i, tx := range txes {
			if txErrors[i] != nil {
				continue
			}
			txBytes, err := tx.MarshalBinary()
			if err != nil {
				return nil, err
			}
			binary.BigEndian.PutUint64(sizeBuf, uint64(len(txBytes)+1))
			l2Message = append(l2Message, sizeBuf...)
			l2Message = append(l2Message, arbos.L2MessageKind_SignedTx)
			l2Message = append(l2Message, txBytes...)
		}
	}
	return &arbostypes.L1IncomingMessage{
		Header: header,
		L2msg:  l2Message,
	}, nil
}

// The caller must hold the createBlocksMutex
func (s *ExecutionEngine) resequenceReorgedMessages(messages []*arbostypes.MessageWithMetadata) {
	if !s.reorgSequencing {
		return
	}

	log.Info("Trying to resequence messages", "number", len(messages))
	lastBlockHeader, err := s.getCurrentHeader()
	if err != nil {
		log.Error("block header not found during resequence", "err", err)
		return
	}

	nextDelayedSeqNum := lastBlockHeader.Nonce.Uint64()

	for _, msg := range messages {
		// Check if the message is non-nil just to be safe
		if msg == nil || msg.Message == nil || msg.Message.Header == nil {
			continue
		}
		header := msg.Message.Header
		if header.RequestId != nil {
			delayedSeqNum := header.RequestId.Big().Uint64()
			if delayedSeqNum != nextDelayedSeqNum {
				log.Info("not resequencing delayed message due to unexpected index", "expected", nextDelayedSeqNum, "found", delayedSeqNum)
				continue
			}
			_, err := s.sequenceDelayedMessageWithBlockMutex(msg.Message, delayedSeqNum)
			if err != nil {
				log.Error("failed to re-sequence old delayed message removed by reorg", "err", err)
			}
			nextDelayedSeqNum += 1
			continue
		}
		if header.Kind != arbostypes.L1MessageType_L2Message || header.Poster != l1pricing.BatchPosterAddress {
			// This shouldn't exist?
			log.Warn("skipping non-standard sequencer message found from reorg", "header", header)
			continue
		}
		// We don't need a batch fetcher as this is an L2 message
		txes, err := arbos.ParseL2Transactions(msg.Message, s.bc.Config().ChainID, nil)
		if err != nil {
			log.Warn("failed to parse sequencer message found from reorg", "err", err)
			continue
		}
		hooks := arbos.NoopSequencingHooks()
		hooks.DiscardInvalidTxsEarly = true
		_, err = s.sequenceTransactionsWithBlockMutex(msg.Message.Header, txes, hooks)
		if err != nil {
			log.Error("failed to re-sequence old user message removed by reorg", "err", err)
			return
		}
	}
}

var ErrSequencerInsertLockTaken = errors.New("insert lock taken")

func (s *ExecutionEngine) sequencerWrapper(sequencerFunc func() (*types.Block, error)) (*types.Block, error) {
	attempts := 0
	for {
		s.createBlocksMutex.Lock()
		block, err := sequencerFunc()
		s.createBlocksMutex.Unlock()
		if !errors.Is(err, ErrSequencerInsertLockTaken) {
			return block, err
		}
		// We got SequencerInsertLockTaken
		// option 1: there was a race, we are no longer main sequencer
		chosenErr := s.streamer.ExpectChosenSequencer()
		if chosenErr != nil {
			return nil, chosenErr
		}
		// option 2: we are in a test without very orderly sequencer coordination
		if !s.bc.Config().ArbitrumChainParams.AllowDebugPrecompiles {
			// option 3: something weird. send warning
			log.Warn("sequence transactions: insert lock takent", "attempts", attempts)
		}
		// options 2/3 fail after too many attempts
		attempts++
		if attempts > 20 {
			return nil, err
		}
		<-time.After(time.Millisecond * 100)
	}
}

func (s *ExecutionEngine) SequenceTransactions(header *arbostypes.L1IncomingMessageHeader, txes types.Transactions, hooks *arbos.SequencingHooks) (*types.Block, error) {
	return s.sequencerWrapper(func() (*types.Block, error) {
		hooks.TxErrors = nil
		return s.sequenceTransactionsWithBlockMutex(header, txes, hooks)
	})
}

func (s *ExecutionEngine) sequenceTransactionsWithBlockMutex(header *arbostypes.L1IncomingMessageHeader, txes types.Transactions, hooks *arbos.SequencingHooks) (*types.Block, error) {
	lastBlockHeader, err := s.getCurrentHeader()
	if err != nil {
		return nil, err
	}

	statedb, err := s.bc.StateAt(lastBlockHeader.Root)
	if err != nil {
		return nil, err
	}

	delayedMessagesRead := lastBlockHeader.Nonce.Uint64()

	startTime := time.Now()
	block, receipts, err := arbos.ProduceBlockAdvanced(
		header,
		txes,
		delayedMessagesRead,
		lastBlockHeader,
		statedb,
		s.bc,
		s.bc.Config(),
		hooks,
	)
	if err != nil {
		return nil, err
	}
	blockCalcTime := time.Since(startTime)
	if len(hooks.TxErrors) != len(txes) {
		return nil, fmt.Errorf("unexpected number of error results: %v vs number of txes %v", len(hooks.TxErrors), len(txes))
	}

	if len(receipts) == 0 {
		return nil, nil
	}

	allTxsErrored := true
	for _, err := range hooks.TxErrors {
		if err == nil {
			allTxsErrored = false
			break
		}
	}
	if allTxsErrored {
		return nil, nil
	}

	msg, err := messageFromTxes(header, txes, hooks.TxErrors)
	if err != nil {
		return nil, err
	}

	msgWithMeta := arbostypes.MessageWithMetadata{
		Message:             msg,
		DelayedMessagesRead: delayedMessagesRead,
	}

	pos, err := s.BlockNumberToMessageCount(lastBlockHeader.Number.Uint64())
	if err != nil {
		return nil, err
	}

	err = s.streamer.WriteMessageFromSequencer(pos, msgWithMeta)
	if err != nil {
		return nil, err
	}

	// Only write the block after we've written the messages, so if the node dies in the middle of this,
	// it will naturally recover on startup by regenerating the missing block.
	err = s.appendBlock(block, statedb, receipts, blockCalcTime)
	if err != nil {
		return nil, err
	}

	if s.validator != nil {
		s.validator.NewBlock(block, lastBlockHeader, msgWithMeta)
	}

	return block, nil
}

func (s *ExecutionEngine) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	_, err := s.sequencerWrapper(func() (*types.Block, error) {
		return s.sequenceDelayedMessageWithBlockMutex(message, delayedSeqNum)
	})
	return err
}

func (s *ExecutionEngine) sequenceDelayedMessageWithBlockMutex(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) (*types.Block, error) {
	currentHeader, err := s.getCurrentHeader()
	if err != nil {
		return nil, err
	}

	expectedDelayed := currentHeader.Nonce.Uint64()

	pos, err := s.BlockNumberToMessageCount(currentHeader.Number.Uint64())
	if err != nil {
		return nil, err
	}

	if expectedDelayed != delayedSeqNum {
		return nil, fmt.Errorf("wrong delayed message sequenced got %d expected %d", delayedSeqNum, expectedDelayed)
	}

	messageWithMeta := arbostypes.MessageWithMetadata{
		Message:             message,
		DelayedMessagesRead: delayedSeqNum + 1,
	}

	err = s.streamer.WriteMessageFromSequencer(pos, messageWithMeta)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	block, statedb, receipts, err := s.createBlockFromNextMessage(&messageWithMeta)
	if err != nil {
		return nil, err
	}

	err = s.appendBlock(block, statedb, receipts, time.Since(startTime))
	if err != nil {
		return nil, err
	}

	log.Info("ExecutionEngine: Added DelayedMessages", "pos", pos, "delayed", delayedSeqNum, "block-header", block.Header())

	return block, nil
}

func (s *ExecutionEngine) GetGenesisBlockNumber() (uint64, error) {
	return s.bc.Config().ArbitrumChainParams.GenesisBlockNum, nil
}

func (s *ExecutionEngine) BlockNumberToMessageCount(blockNum uint64) (arbutil.MessageIndex, error) {
	genesis, err := s.GetGenesisBlockNumber()
	if err != nil {
		return 0, err
	}
	return arbutil.BlockNumberToMessageCount(blockNum, genesis), nil
}

func (s *ExecutionEngine) MessageCountToBlockNumber(messageNum arbutil.MessageIndex) (int64, error) {
	genesis, err := s.GetGenesisBlockNumber()
	if err != nil {
		return 0, err
	}
	return arbutil.MessageCountToBlockNumber(messageNum, genesis), nil
}

// must hold createBlockMutex
func (s *ExecutionEngine) createBlockFromNextMessage(msg *arbostypes.MessageWithMetadata) (*types.Block, *state.StateDB, types.Receipts, error) {
	currentBlock := s.bc.CurrentBlock()
	if currentBlock == nil {
		return nil, nil, nil, errors.New("failed to get current block")
	}

	err := s.bc.RecoverState(currentBlock)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to recover block %v state: %w", currentBlock.Number(), err)
	}

	currentHeader := currentBlock.Header()

	statedb, err := s.bc.StateAt(currentHeader.Root)
	if err != nil {
		return nil, nil, nil, err
	}
	statedb.StartPrefetcher("TransactionStreamer")
	defer statedb.StopPrefetcher()

	block, receipts, err := arbos.ProduceBlock(
		msg.Message,
		msg.DelayedMessagesRead,
		currentHeader,
		statedb,
		s.bc,
		s.bc.Config(),
		s.streamer.FetchBatch,
	)

	return block, statedb, receipts, err
}

// must hold createBlockMutex
func (s *ExecutionEngine) appendBlock(block *types.Block, statedb *state.StateDB, receipts types.Receipts, duration time.Duration) error {
	var logs []*types.Log
	for _, receipt := range receipts {
		logs = append(logs, receipt.Logs...)
	}
	status, err := s.bc.WriteBlockAndSetHeadWithTime(block, receipts, logs, statedb, true, duration)
	if err != nil {
		return err
	}
	if status == core.SideStatTy {
		return errors.New("geth rejected block as non-canonical")
	}
	return nil
}

func (s *ExecutionEngine) DigestMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) error {
	if !s.createBlocksMutex.TryLock() {
		return errors.New("createBlock mutex held")
	}
	defer s.createBlocksMutex.Unlock()
	return s.digestMessageWithBlockMutex(num, msg)
}

func (s *ExecutionEngine) sendReceiptsToListener(receipts types.Receipts) {
	var logs []*types.Log
	for _, receipt := range receipts {
		logs = append(logs, receipt.Logs...)
	}
	if receipts.Len() > 0 {
		go func() {
			for key, _ := range s.receiptschs {
				s.mutex.Lock()
				s.receiptschs[key] <- receipts
				s.mutex.Unlock()
			}
		}()
	}
	if len(logs) > 0 {
		go func() {
			for key, _ := range s.headerchs {
				s.mutex.Lock()
				s.headerchs[key] <- logs
				s.mutex.Unlock()
			}
		}()
	}
}

func (s *ExecutionEngine) digestMessageWithBlockMutex(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) error {
	currentHeader, err := s.getCurrentHeader()
	if err != nil {
		return err
	}
	expNum, err := s.BlockNumberToMessageCount(currentHeader.Number.Uint64())
	if err != nil {
		return err
	}
	if expNum != num {
		return fmt.Errorf("wrong message number in digest got %d expected %d", num, expNum)
	}

	startTime := time.Now()
	block, statedb, receipts, err := s.createBlockFromNextMessage(msg)
	if err != nil {
		return err
	}
	go s.sendReceiptsToListener(receipts)
	err = s.appendBlock(block, statedb, receipts, time.Since(startTime))
	if err != nil {
		return err
	}

	if s.validator != nil {
		s.validator.NewBlock(block, currentHeader, *msg)
	}

	if time.Now().After(s.nextScheduledVersionCheck) {
		s.nextScheduledVersionCheck = time.Now().Add(time.Minute)
		arbState, err := arbosState.OpenSystemArbosState(statedb, nil, true)
		if err != nil {
			return err
		}
		version, timestampInt, err := arbState.GetScheduledUpgrade()
		if err != nil {
			return err
		}
		var timeUntilUpgrade time.Duration
		var timestamp time.Time
		if timestampInt == 0 {
			// This upgrade will take effect in the next block
			timestamp = time.Now()
		} else {
			// This upgrade is scheduled for the future
			timestamp = time.Unix(int64(timestampInt), 0)
			timeUntilUpgrade = time.Until(timestamp)
		}
		maxSupportedVersion := params.ArbitrumDevTestChainConfig().ArbitrumChainParams.InitialArbOSVersion
		logLevel := log.Warn
		if timeUntilUpgrade < time.Hour*24 {
			logLevel = log.Error
		}
		if version > maxSupportedVersion {
			logLevel(
				"you need to update your node to the latest version before this scheduled ArbOS upgrade",
				"timeUntilUpgrade", timeUntilUpgrade,
				"upgradeScheduledFor", timestamp,
				"maxSupportedArbosVersion", maxSupportedVersion,
				"pendingArbosUpgradeVersion", version,
			)
		}
	}

	sharedmetrics.UpdateSequenceNumberInBlockGauge(num)
	s.latestBlockMutex.Lock()
	s.latestBlock = block
	s.latestBlockMutex.Unlock()
	select {
	case s.newBlockNotifier <- struct{}{}:
	default:
	}
	return nil
}

var upgrader = websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096}

type NewHeader struct {
	Logs []types.Log `json:"logs"`
}

type NewReceipt struct {
	TxHash          common.Hash    `json:"txHash"`
	ContractAddress common.Address `json:"contractAddress"`
	GasUsed         uint64         `json:"gasUsed"`
	Status          uint64         `json:"status"`
	Logs            []types.Log    `json:"logs"`
}

type NewReceipts struct {
	Receipts []NewReceipt `json:"receipts"`
}

func (s *ExecutionEngine) newReceipts(w http.ResponseWriter, r *http.Request) {
	log.Info("create handler for new receipts")
	index := s.globalCountR
	s.receiptschs[index] = make(chan types.Receipts, 100)
	s.globalCountR += 1
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Info("fail to setup ws hanlder")
		return
	}
	defer c.Close()
	defer delete(s.receiptschs, index)
	for {
		select {
		case receipts := <-s.receiptschs[index]:
			newReceipts := NewReceipts{}
			for _, receipt := range receipts {
				var logs []types.Log
				for _, log := range receipt.Logs {
					logs = append(logs, types.Log{
						Address:     log.Address,
						Topics:      log.Topics,
						Data:        log.Data,
						BlockNumber: log.BlockNumber,
						TxHash:      log.TxHash,
						TxIndex:     log.TxIndex,
						BlockHash:   log.BlockHash,
						Index:       log.Index,
						Removed:     log.Removed,
					})
				}
				newReceipts.Receipts = append(newReceipts.Receipts, NewReceipt{
					TxHash:          receipt.TxHash,
					ContractAddress: receipt.ContractAddress,
					GasUsed:         receipt.GasUsed,
					Status:          receipt.Status,
					Logs:            logs,
				})
			}
			err = c.WriteJSON(newReceipts)
			if err != nil {
				log.Info("fail to write json")
				return
			}
		}
	}

}

func (s *ExecutionEngine) newHeader(w http.ResponseWriter, r *http.Request) {
	log.Info("create handler for new headers")
	index := s.globalCountH
	s.headerchs[index] = make(chan []*types.Log, 100)
	s.globalCountH += 1
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Info("fail to setup ws hanlder")
		return
	}
	defer c.Close()
	defer delete(s.headerchs, index)
	for {
		select {
		case logs := <-s.headerchs[index]:
			newHead := NewHeader{}
			for ind := 0; ind < len(logs); ind++ {
				log := logs[ind]
				newHead.Logs = append(newHead.Logs, types.Log{
					Address:     log.Address,
					Topics:      log.Topics,
					Data:        log.Data,
					BlockNumber: log.BlockNumber,
					TxHash:      log.TxHash,
					TxIndex:     log.TxIndex,
					BlockHash:   log.BlockHash,
					Index:       log.Index,
					Removed:     log.Removed,
				})
			}
			payload := newHead
			payloadJson, _ := json.Marshal(payload)
			serr := c.WriteMessage(websocket.TextMessage, payloadJson)
			if serr != nil {
				log.Info(serr.Error())
				return
			}

		}
	}
	return
}

func (s *ExecutionEngine) Start(ctx_in context.Context) {
	s.StopWaiter.Start(ctx_in, s)
	log.Info("start up ws Server.")
	http.HandleFunc("/header", s.newHeader)
	http.HandleFunc("/receipts", s.newReceipts)
	go http.ListenAndServe(":8086", nil)
	log.Info("ws Server started.")
	s.LaunchThread(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case resequence := <-s.resequenceChan:
				s.resequenceReorgedMessages(resequence)
				s.createBlocksMutex.Unlock()
			}
		}
	})
	s.LaunchThread(func(ctx context.Context) {
		var lastBlock *types.Block
		for {
			select {
			case <-s.newBlockNotifier:
			case <-ctx.Done():
				return
			}
			s.latestBlockMutex.Lock()
			block := s.latestBlock
			s.latestBlockMutex.Unlock()
			if block != lastBlock && block != nil {
				log.Info(
					"created block",
					"l2Block", block.Number(),
					"l2BlockHash", block.Hash(),
				)
				lastBlock = block
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	})
}
