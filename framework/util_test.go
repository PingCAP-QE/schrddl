package framework

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRoundToSixDecimalsWithLargeNumber(t *testing.T) {
	f := "1.23456789e18"
	rf := RoundToOneDecimals(f)
	require.Equal(t, "1.2e+18", rf)
}

func TestRoundToSixDecimalsWithSmallNumber(t *testing.T) {
	f := "1.23456789e-5"
	rf := RoundToOneDecimals(f)
	require.Equal(t, "0.0", rf)
}

func TestRoundToSixDecimalsWithZero(t *testing.T) {
	f := "0"
	rf := RoundToOneDecimals(f)
	require.Equal(t, "0.0", rf)
}

func TestRoundToSixDecimalsWithNegativeNumber(t *testing.T) {
	f := "-1.23456789e18"
	rf := RoundToOneDecimals(f)
	require.Equal(t, "-1.2e+18", rf)
}

func TestRoundToSixDecimalsWithNonNumber(t *testing.T) {
	f := "abc"
	rf := RoundToOneDecimals(f)
	require.Equal(t, "", rf)
}

func TestRoundToSixDecimalsWithSmallFloatNumber(t *testing.T) {
	f := "12345.123456788"
	rf := RoundToOneDecimals(f)
	require.Equal(t, "12345.1", rf)
}
