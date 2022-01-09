//
// Copyright 2021, Offchain Labs, Inc. All rights reserved.
//

package precompiles

import (
	"errors"
	"math/big"

	"github.com/offchainlabs/arbstate/arbos/arbosState"
	"github.com/offchainlabs/arbstate/arbos/burn"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

// A precompile wrapper for those not allowed in production
type DebugPrecompile struct {
	precompile ArbosPrecompile
}

// create a debug-only precompile wrapper
func debugOnly(address addr, impl ArbosPrecompile) (addr, ArbosPrecompile) {
	return address, &DebugPrecompile{impl}
}

func (wrapper *DebugPrecompile) Call(
	input []byte,
	precompileAddress common.Address,
	actingAsAddress common.Address,
	caller common.Address,
	value *big.Int,
	readOnly bool,
	gasSupplied uint64,
	evm *vm.EVM,
) ([]byte, uint64, error) {

	debugMode := evm.ChainConfig().DebugMode()

	if debugMode {
		con := wrapper.precompile
		return con.Call(input, precompileAddress, actingAsAddress, caller, value, readOnly, gasSupplied, evm)
	} else {
		// take all gas
		return nil, 0, errors.New("Debug precompiles are disabled")
	}
}

func (wrapper *DebugPrecompile) Precompile() Precompile {
	return wrapper.precompile.Precompile()
}

// A precompile wrapper for those only chain owners may use
type OwnerPrecompile struct {
	precompile ArbosPrecompile
}

func ownerOnly(address addr, impl ArbosPrecompile) (addr, ArbosPrecompile) {
	return address, &OwnerPrecompile{impl}
}

func (wrapper *OwnerPrecompile) Call(
	input []byte,
	precompileAddress common.Address,
	actingAsAddress common.Address,
	caller common.Address,
	value *big.Int,
	readOnly bool,
	gasSupplied uint64,
	evm *vm.EVM,
) ([]byte, uint64, error) {
	con := wrapper.precompile

	if gasSupplied < 3*params.SloadGas {
		// the user can't pay for the ownership check
		return nil, 0, vm.ErrOutOfGas
	}
	burner := &burn.SystemBurner{} // not the usual metered burner since we don't want to charge owners
	owners := arbosState.OpenArbosState(evm.StateDB, burner).ChainOwners()
	if !owners.IsMember(caller) {
		gasLeft := gasSupplied - burner.Burned()
		return nil, gasLeft, errors.New("unauthorized caller to access-controlled method")
	}

	// we don't deduct gas since we don't want to charge the owner
	return con.Call(input, precompileAddress, actingAsAddress, caller, value, readOnly, gasSupplied, evm)

}

func (wrapper *OwnerPrecompile) Precompile() Precompile {
	con := wrapper.precompile
	return con.Precompile()
}
