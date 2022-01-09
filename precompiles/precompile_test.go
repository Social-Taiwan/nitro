//
// Copyright 2021, Offchain Labs, Inc. All rights reserved.
//

package precompiles

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/offchainlabs/arbstate/arbos"
	"github.com/offchainlabs/arbstate/arbos/arbosState"
	"github.com/offchainlabs/arbstate/arbos/burn"
	"github.com/offchainlabs/arbstate/arbos/storage"
	templates "github.com/offchainlabs/arbstate/solgen/go/precompilesgen"
)

func TestEvents(t *testing.T) {
	blockNumber := 1024

	chainConfig := params.ArbitrumTestChainConfig()
	statedb := storage.NewMemoryBackedStateDB()
	context := vm.BlockContext{
		BlockNumber: big.NewInt(int64(blockNumber)),
		GasLimit:    ^uint64(0),
	}

	// open now to induce an upgrade
	arbosState.OpenArbosState(statedb, &burn.SystemBurner{})

	// create a minimal evm that supports just enough to create logs
	evm := vm.NewEVM(context, vm.TxContext{}, statedb, chainConfig, vm.Config{})
	evm.ProcessingHook = &arbos.TxProcessor{}

	debugContractAddr := common.HexToAddress("ff")
	contract := Precompiles()[debugContractAddr]

	var method PrecompileMethod

	for _, available := range contract.Precompile().methods {
		if available.name == "Events" {
			method = available
			break
		}
	}

	zeroHash := crypto.Keccak256([]byte{0x00})
	trueHash := common.Hash{}.Bytes()
	falseHash := common.Hash{}.Bytes()
	trueHash[31] = 0x01

	var data []byte
	payload := [][]byte{
		method.template.ID, // select the `Events` method
		falseHash,          // set the flag to false
		zeroHash,           // set the value to something known
	}
	for _, bytes := range payload {
		data = append(data, bytes...)
	}

	caller := common.HexToAddress("aaaaaaaabbbbbbbbccccccccdddddddd")
	number := big.NewInt(0x9364)

	output, gasLeft, err := contract.Call(
		data,
		debugContractAddr,
		debugContractAddr,
		caller,
		number,
		false,
		^uint64(0),
		evm,
	)
	Require(t, err, "call failed")

	burnedToEvents := ^uint64(0) - gasLeft - storage.StorageReadCost // the ArbOS version check costs a read
	if burnedToEvents != 3768 {
		t.Fatal("burned", burnedToEvents, "instead of", 3768, "gas")
	}

	outputAddr := common.BytesToAddress(output[:32])
	outputData := new(big.Int).SetBytes(output[32:])

	if outputAddr != caller {
		t.Fatal("unexpected output address", outputAddr, "instead of", caller)
	}
	if outputData.Cmp(number) != 0 {
		t.Fatal("unexpected output number", outputData, "instead of", number)
	}

	//nolint:errcheck
	logs := evm.StateDB.(*state.StateDB).Logs()
	for _, log := range logs {
		if log.Address != debugContractAddr {
			t.Fatal("address mismatch:", log.Address, "vs", debugContractAddr)
		}
		if log.BlockNumber != uint64(blockNumber) {
			t.Fatal("block number mismatch:", log.BlockNumber, "vs", blockNumber)
		}
		t.Log("topic", len(log.Topics), log.Topics)
		t.Log("data ", len(log.Data), log.Data)
	}

	basicTopics := logs[0].Topics
	mixedTopics := logs[1].Topics

	if !bytes.Equal(basicTopics[1].Bytes(), zeroHash) || !bytes.Equal(mixedTopics[2].Bytes(), zeroHash) {
		t.Fatal("indexing a bytes32 didn't work")
	}
	if !bytes.Equal(mixedTopics[1].Bytes(), falseHash) {
		t.Fatal("indexing a bool didn't work")
	}
	if !bytes.Equal(mixedTopics[3].Bytes(), caller.Hash().Bytes()) {
		t.Fatal("indexing an address didn't work")
	}

	ArbDebugInfo, cerr := templates.NewArbDebug(common.Address{}, nil)
	basic, berr := ArbDebugInfo.ParseBasic(*logs[0])
	mixed, merr := ArbDebugInfo.ParseMixed(*logs[1])
	if cerr != nil || berr != nil || merr != nil {
		t.Fatal("failed to parse event logs", "\nprecompile:", cerr, "\nbasic:", berr, "\nmixed:", merr)
	}

	if basic.Flag != true || !bytes.Equal(basic.Value[:], zeroHash) {
		t.Fatal("event Basic's data isn't correct")
	}
	if mixed.Flag != false || mixed.Not != true || !bytes.Equal(mixed.Value[:], zeroHash) {
		t.Fatal("event Mixed's data isn't correct")
	}
	if mixed.Conn != debugContractAddr || mixed.Caller != caller {
		t.Fatal("event Mixed's data isn't correct")
	}
}

func TestEventCosts(t *testing.T) {
	debugContractAddr := common.HexToAddress("ff")
	contract := Precompiles()[debugContractAddr]

	//nolint:errcheck
	impl := contract.Precompile().implementer.Interface().(*ArbDebug)

	testBytes := [...][]byte{
		nil,
		{0x01},
		{0x02, 0x32, 0x24, 0x48},
		common.Hash{}.Bytes(),
		common.Hash{}.Bytes(),
	}
	testBytes[4] = append(testBytes[4], common.Hash{}.Bytes()...)

	tests := [...]uint64{
		impl.StoreGasCost(true, addr{}, big.NewInt(24), common.Hash{}, testBytes[0]),
		impl.StoreGasCost(false, addr{}, big.NewInt(8), common.Hash{}, testBytes[1]),
		impl.StoreGasCost(false, addr{}, big.NewInt(8), common.Hash{}, testBytes[2]),
		impl.StoreGasCost(true, addr{}, big.NewInt(32), common.Hash{}, testBytes[3]),
		impl.StoreGasCost(true, addr{}, big.NewInt(64), common.Hash{}, testBytes[4]),
	}

	expected := [5]uint64{}

	for i, bytes := range testBytes {
		baseCost := params.LogGas + 3*params.LogTopicGas
		addrCost := 32 * params.LogDataGas
		hashCost := 32 * params.LogDataGas

		sizeBytes := 32
		offsetBytes := 32
		storeBytes := sizeBytes + offsetBytes + len(bytes)
		storeBytes = storeBytes + 31 - (storeBytes+31)%32 // round up to a multiple of 32
		storeCost := uint64(storeBytes) * params.LogDataGas

		expected[i] = baseCost + addrCost + hashCost + storeCost
	}

	if tests != expected {
		t.Fatal("Events are mispriced\nexpected:", expected, "\nbut have:", tests)
	}
}

type FatalBurner struct {
	t       *testing.T
	count   uint64
	gasLeft uint64
}

func NewFatalBurner(t *testing.T, limit uint64) FatalBurner {
	return FatalBurner{t, 0, limit}
}

func (burner FatalBurner) Burn(amount uint64) error {
	burner.t.Helper()
	burner.count += 1
	if burner.gasLeft < amount {
		Fail(burner.t, "out of gas after", burner.count, "burns")
	}
	burner.gasLeft -= amount
	return nil
}
