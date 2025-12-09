package sqlgenerator

import (
	"fmt"
	"math/rand"
	"strconv"
)

// StatsConfig configuration for statistics validation
type StatsConfig struct {
	// Data distribution configuration
	SkewFactor      float64 // Zipfian distribution skew factor, higher value = more skewed
	CorrelationRate float64 // Column correlation rate (0-1)
	NullRate        float64 // NULL value rate (0-1)
	BoundaryValue   bool    // Whether to generate boundary values

	// Column correlation mapping: column name -> correlated column name
	ColumnCorrelations map[string]string

	// Zipfian generators (per column)
	zipfGenerators map[string]*rand.Zipf
}

// NewStatsConfig creates a new statistics configuration
func NewStatsConfig() *StatsConfig {
	return &StatsConfig{
		SkewFactor:         1.0, // Default: no skew
		CorrelationRate:    0.0,
		NullRate:           0.0,
		BoundaryValue:      false,
		ColumnCorrelations: make(map[string]string),
		zipfGenerators:     make(map[string]*rand.Zipf),
	}
}

// SetSkewFactor sets the skew factor
func (sc *StatsConfig) SetSkewFactor(factor float64) {
	sc.SkewFactor = factor
}

// SetNullRate sets the NULL value rate
func (sc *StatsConfig) SetNullRate(rate float64) {
	sc.NullRate = rate
}

// AddCorrelation adds column correlation
func (sc *StatsConfig) AddCorrelation(colName, correlatedWith string) {
	sc.ColumnCorrelations[colName] = correlatedWith
}

// GetZipfGenerator gets or creates a Zipfian generator
func (sc *StatsConfig) GetZipfGenerator(colName string, maxValue uint64) *rand.Zipf {
	if sc.SkewFactor <= 1.0 {
		return nil // No skew
	}

	if gen, ok := sc.zipfGenerators[colName]; ok {
		return gen
	}

	// Create a new Zipfian generator
	r := rand.New(rand.NewSource(rand.Int63()))
	gen := rand.NewZipf(r, sc.SkewFactor, 1.0, maxValue)
	sc.zipfGenerators[colName] = gen
	return gen
}

// SetStatsConfig sets statistics configuration for State
func (s *State) SetStatsConfig(config *StatsConfig) {
	if s.env == nil {
		s.env = &Env{}
	}
	if s.env.Elem == nil {
		s.env.Elem = &Elem{}
	}
	s.env.StatsConfig = config
}

// GetStatsConfig gets statistics configuration from State
func (s *State) GetStatsConfig() *StatsConfig {
	if s.env == nil || s.env.Elem == nil || s.env.StatsConfig == nil {
		return NewStatsConfig()
	}
	return s.env.StatsConfig
}

// RandomValueWithSkew generates a random value with skew distribution support
func (c *Column) RandomValueWithSkew(config *StatsConfig, rowIdx int) string {
	if !c.IsNotNull && config != nil && rand.Float64() < config.NullRate {
		return "null"
	}

	// Check if Zipfian distribution should be used
	if config != nil && config.SkewFactor > 1.0 {
		switch c.Tp {
		case ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt, ColumnTypeBigInt:
			zipf := config.GetZipfGenerator(c.Name, 1000000)
			if zipf != nil {
				val := zipf.Uint64()
				if c.IsUnsigned {
					// For unsigned integers, clamp to the valid range
					var maxVal uint64
					switch c.Tp {
					case ColumnTypeTinyInt:
						maxVal = 255
					case ColumnTypeSmallInt:
						maxVal = 65535
					case ColumnTypeMediumInt:
						maxVal = 16777215
					case ColumnTypeInt:
						maxVal = 4294967295
					case ColumnTypeBigInt:
						// BigInt unsigned can handle the full range
						return strconv.FormatUint(val, 10)
					}
					if val > maxVal {
						val = val % (maxVal + 1)
					}
					return strconv.FormatUint(val, 10)
				}
				// For signed integers, map to the valid range including negatives
				var minVal int64
				var rangeSize uint64
				switch c.Tp {
				case ColumnTypeTinyInt:
					minVal = -128
					rangeSize = 256 // 127 - (-128) + 1
				case ColumnTypeSmallInt:
					minVal = -32768
					rangeSize = 65536 // 32767 - (-32768) + 1
				case ColumnTypeMediumInt:
					minVal = -8388608
					rangeSize = 16777216 // 8388607 - (-8388608) + 1
				case ColumnTypeInt:
					minVal = -2147483648
					rangeSize = 4294967296 // 2147483647 - (-2147483648) + 1
				case ColumnTypeBigInt:
					minVal = -9223372036854775808
					// For BIGINT, rangeSize would be 18446744073709551616, which exceeds uint64 max
					// Use uint64 max value instead, which is effectively the same for modulo
					rangeSize = 18446744073709551615 // uint64 max
				}
				// Map zipf value (0-1000000) to the column's valid range
				// Use uint64 for modulo to avoid overflow issues
				mappedVal := val % rangeSize
				resultVal := minVal + int64(mappedVal)
				return strconv.FormatInt(resultVal, 10)
			}
		}
	}

	// Default: use original method
	return c.RandomValue()
}

// RandomValueWithCorrelation generates a random value with correlation support
func (c *Column) RandomValueWithCorrelation(config *StatsConfig, correlatedValue interface{}) string {
	if !c.IsNotNull && config != nil && rand.Float64() < config.NullRate {
		return "null"
	}

	// If correlation exists, generate based on correlated column value
	if config != nil && correlatedValue != nil {
		switch c.Tp {
		case ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt, ColumnTypeBigInt:
			// Generate based on correlated value
			var baseVal int64
			switch v := correlatedValue.(type) {
			case int64:
				baseVal = v
			case int:
				baseVal = int64(v)
			case string:
				// Extract numeric value from string
				if len(v) > 0 {
					baseVal = int64(v[0]) % 1000
				}
			}
			// Add some randomness but maintain correlation
			correlatedVal := baseVal + int64(rand.Intn(10))
			return strconv.FormatInt(correlatedVal, 10)

		case ColumnTypeChar, ColumnTypeVarchar:
			// String correlation - use ASCII-only to avoid charset issues
			if baseStr, ok := correlatedValue.(string); ok {
				// Extract only ASCII characters from baseStr to avoid encoding issues
				asciiBase := ""
				for _, r := range baseStr {
					if r < 128 {
						asciiBase += string(r)
					}
				}
				if len(asciiBase) == 0 {
					asciiBase = "val"
				}
				return fmt.Sprintf("'corr_%s_%d'", asciiBase, rand.Intn(10))
			}
		}
	}

	// Default: use original method
	return c.RandomValue()
}

// getBitSize gets the bit size of the column
func (c *Column) getBitSize() int {
	switch c.Tp {
	case ColumnTypeTinyInt:
		return 8
	case ColumnTypeSmallInt:
		return 16
	case ColumnTypeMediumInt:
		return 24
	case ColumnTypeInt:
		return 32
	case ColumnTypeBigInt:
		return 64
	default:
		return 32
	}
}

// GenerateRowWithStats generates a row with statistics-aware data generation
func (t *Table) GenerateRowWithStats(cols Columns, config *StatsConfig, rowIdx int) []string {
	values := make([]string, len(cols))
	correlationValues := make(map[string]interface{})

	for i, col := range cols {
		// Check if correlation exists
		if config != nil {
			if correlatedWith, ok := config.ColumnCorrelations[col.Name]; ok {
				// Find the correlated column value
				found := false
				for _, c := range cols {
					if c.Name == correlatedWith {
						// Use the correlated column value
						if corrVal, exists := correlationValues[correlatedWith]; exists {
							values[i] = col.RandomValueWithCorrelation(config, corrVal)
						} else {
							// If correlated column not generated yet, generate it first
							corrVal := c.RandomValueWithSkew(config, rowIdx)
							correlationValues[correlatedWith] = corrVal
							values[i] = col.RandomValueWithCorrelation(config, corrVal)
						}
						found = true
						break
					}
				}
				if found {
					continue
				}
			}
		}

		// Use skewed data generation
		values[i] = col.RandomValueWithSkew(config, rowIdx)

		// Save value for correlation
		if config != nil {
			correlationValues[col.Name] = values[i]
		}
	}

	return values
}
