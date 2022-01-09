//
// Copyright 2021, Offchain Labs, Inc. All rights reserved.
//

package precompiles

import (
	"errors"

	"github.com/ethereum/go-ethereum/params"
)

type ArbAggregator struct {
	Address addr
}

func (con ArbAggregator) GetFeeCollector(c ctx, evm mech, aggregator addr) (addr, error) {
	if err := c.burn(params.SloadGas); err != nil {
		return addr{}, err
	}
	return c.state.L1PricingState().AggregatorFeeCollector(aggregator), nil
}

func (con ArbAggregator) GetDefaultAggregator(c ctx, evm mech) (addr, error) {
	if err := c.burn(params.SloadGas); err != nil {
		return addr{}, err
	}
	return c.state.L1PricingState().DefaultAggregator(), nil
}

func (con ArbAggregator) GetPreferredAggregator(c ctx, evm mech, address addr) (addr, bool, error) {
	if err := c.burn(params.SloadGas); err != nil {
		return addr{}, false, err
	}
	res, exists := c.state.L1PricingState().PreferredAggregator(address)
	return res, exists, nil
}

func (con ArbAggregator) GetTxBaseFee(c ctx, evm mech, aggregator addr) (huge, error) {
	if err := c.burn(params.SloadGas); err != nil {
		return nil, err
	}
	return c.state.L1PricingState().FixedChargeForAggregatorL1Gas(aggregator), nil
}

func (con ArbAggregator) SetFeeCollector(c ctx, evm mech, aggregator addr, newFeeCollector addr) error {
	if err := c.burn(params.SloadGas + params.SstoreSetGas); err != nil {
		return err
	}
	l1State := c.state.L1PricingState()
	if (c.caller != aggregator) && (c.caller != l1State.AggregatorFeeCollector(aggregator)) {
		// only the aggregator and its current fee collector can change the aggregator's fee collector
		return errors.New("non-authorized c.caller in ArbAggregator.SetFeeCollector")
	}
	l1State.SetAggregatorFeeCollector(aggregator, newFeeCollector)
	return nil
}

func (con ArbAggregator) SetDefaultAggregator(c ctx, evm mech, newDefault addr) error {
	if err := c.burn(params.SstoreSetGas); err != nil {
		return err
	}
	c.state.L1PricingState().SetDefaultAggregator(newDefault)
	return nil
}

func (con ArbAggregator) SetPreferredAggregator(c ctx, evm mech, prefAgg addr) error {
	if err := c.burn(params.SstoreSetGas); err != nil {
		return err
	}
	c.state.L1PricingState().SetPreferredAggregator(c.caller, prefAgg)
	return nil
}

func (con ArbAggregator) SetTxBaseFee(c ctx, evm mech, aggregator addr, feeInL1Gas huge) error {
	if err := c.burn(params.SstoreSetGas); err != nil {
		return err
	}
	c.state.L1PricingState().SetFixedChargeForAggregatorL1Gas(aggregator, feeInL1Gas)
	return nil
}
