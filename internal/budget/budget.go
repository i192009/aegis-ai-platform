// Package budget implements fixed-precision cost and reservation decisions.
package budget

import (
	"errors"
	"math/big"
)

const tokensPerPriceUnit = 1_000_000

var ErrExceeded = errors.New("tenant budget exceeded")

// CostMicroUSD calculates provider cost and rounds fractional micro-USD upward.
func CostMicroUSD(promptTokens, completionTokens, inputPrice, outputPrice int64) (int64, error) {
	if promptTokens < 0 || completionTokens < 0 || inputPrice < 0 || outputPrice < 0 {
		return 0, errors.New("tokens and prices must not be negative")
	}
	input := new(big.Int).Mul(big.NewInt(promptTokens), big.NewInt(inputPrice))
	output := new(big.Int).Mul(big.NewInt(completionTokens), big.NewInt(outputPrice))
	total := input.Add(input, output)
	divisor := big.NewInt(tokensPerPriceUnit)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(total, divisor, remainder)
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, errors.New("calculated cost exceeds supported range")
	}
	return quotient.Int64(), nil
}

// CanReserve prevents committed usage and active reservations from exceeding a limit.
// A zero limit means the platform administrator has not enabled monetary admission.
func CanReserve(limit, committed, activeReservations, requested int64) error {
	if limit == 0 {
		return nil
	}
	if limit < 0 || committed < 0 || activeReservations < 0 || requested < 0 {
		return errors.New("budget values must not be negative")
	}
	if requested > limit || committed > limit-requested || activeReservations > limit-requested-committed {
		return ErrExceeded
	}
	return nil
}
