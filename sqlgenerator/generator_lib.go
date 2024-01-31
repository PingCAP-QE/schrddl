// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlgenerator

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func And(fns ...Fn) Fn {
	ret := defaultFn()
	ret.Info = "And"
	ret.Gen = func(state *State) (string, error) {
		var resStr strings.Builder
		for i, f := range fns {
			Assert(state.GetPrerequisite(f)(state))
			if i != 0 && !f.Equal(PostHandleWith) {
				resStr.WriteString(" ")
			}
			res, err := f.Eval(state)
			if err != nil {
				log.L().Debug("and() error", zap.String("fn", f.Info), zap.Error(err))
				return "", err
			}
			if !f.Equal(PostHandleWith) {
				resStr.WriteString(strings.Trim(res, " "))
			}
		}
		return resStr.String(), nil
	}
	return ret
}

func Or(fns ...Fn) Fn {
	ret := defaultFn()
	ret.Info = "Or"
	ret.Gen = func(state *State) (string, error) {
		var fnNames []string
		var errs []error
		for len(fns) > 0 {
			chosenFnIdx := randSelectByWeight(state, fns)
			if chosenFnIdx == -1 {
				return NoneBecauseOf(fmt.Errorf("or exhausted")).Eval(state)
			}
			chosenFn := fns[chosenFnIdx]
			rs, err := chosenFn.Eval(state)
			if err != nil {
				fnNames = append(fnNames, chosenFn.Info)
				errs = append(errs, err)
				fns[len(fns)-1], fns[chosenFnIdx] = fns[chosenFnIdx], fns[len(fns)-1]
				fns = fns[:len(fns)-1]
				continue
			}
			return rs, nil
		}
		log.L().Debug("or() error", zap.Strings("fns", fnNames), zap.Errors("errors", errs))
		return "", fmt.Errorf("or exhausted")
	}
	return ret
}

// Str is a Fn which simply returns str.
func Str(str string) Fn {
	ret := defaultFn()
	ret.Info = "Str"
	ret.Gen = func(_ *State) (string, error) {
		return str, nil
	}
	return ret
}

func Strf(str string, fns ...Fn) Fn {
	if len(fns) == 0 {
		return Str(str)
	}
	ss := strings.Split(str, "[%fn]")
	if len(ss) != len(fns)+1 {
		panic(fmt.Sprintf("[param count mismatched] str: %s", str))
	}
	strs := make([]Fn, 0, 2*len(ss)-1)
	for i := 0; i < len(fns); i++ {
		strs = append(strs, Str(ss[i]))
		strs = append(strs, fns[i])
		if i == len(fns)-1 {
			strs = append(strs, Str(ss[i+1]))
		}
	}
	return And(strs...)
}

func Strs(strs ...string) Fn {
	ret := defaultFn()
	ret.Info = "Strs"
	ret.Gen = func(state *State) (string, error) {
		return strings.Join(strs, " "), nil
	}
	return ret
}

func If(condition bool, fn Fn) Fn {
	if condition {
		return fn
	}
	return Empty
}

func Repeat(fn Fn, sep Fn) Fn {
	ret := defaultFn()
	ret.Info = "Repeat"
	ret.Gen = func(state *State) (string, error) {
		var resStr strings.Builder
		count := randGenRepeatCount(state, fn)
		for i := 0; i < count; i++ {
			if !state.GetPrerequisite(fn)(state) {
				break
			}
			res, err := fn.Eval(state)
			if err != nil {
				log.L().Debug("repeat() error, skip the rest", zap.Error(err))
			}
			s := strings.Trim(res, " \n\t")
			if len(s) == 0 {
				if i == 0 {
					return "", fmt.Errorf("repeat: %v", err)
				}
				break
			}
			if i != 0 {
				sepRes, err := sep.Eval(state)
				if err != nil {
					return "", fmt.Errorf("repeat sep: %s", err.Error())
				}
				resStr.WriteString(" ")
				resStr.WriteString(sepRes)
			}
			resStr.WriteString(s)
		}
		return resStr.String(), nil
	}
	return ret
}

func RepeatCount(fn Fn, cnt int, sep Fn) Fn {
	if cnt == 0 {
		return Empty
	}
	fns := make([]Fn, 0, 2*cnt-1)
	for i := 0; i < cnt; i++ {
		fns = append(fns, fn)
		if i != cnt-1 {
			fns = append(fns, sep)
		}
	}
	return And(fns...)
}

func Join(fns []Fn, sep Fn) Fn {
	ret := make([]Fn, 0, 2*len(fns)-1)
	for i, f := range fns {
		if i != 0 {
			ret = append(ret, sep)
		}
		ret = append(ret, f)
	}
	return And(ret...)
}

var Empty = NewFn(func(state *State) Fn {
	return Str("")
})

func None(msg string) Fn {
	return NoneBecauseOf(fmt.Errorf(msg))
}

func NoneBecauseOf(err error) Fn {
	ret := defaultFn()
	ret.Info = "NoneBecauseOf"
	ret.Gen = func(state *State) (string, error) {
		return "", fmt.Errorf("none: %s", err.Error())
	}
	return ret
}

func Opt(fn Fn) Fn {
	ret := defaultFn()
	ret.Gen = func(state *State) (string, error) {
		total := 1 + state.GetWeight(fn)
		if rand.Intn(total) == 0 {
			return "", nil
		}
		return fn.Eval(state)
	}
	return ret
}

func Num(v int) string {
	return strconv.FormatInt(int64(v), 10)
}

var UbigInt = []uint64{
	0, 1, 2, 9223372036854775807, 9223372036854775808, 18446744073709551615,
}

var CornerCaseUnsignedNumMap = map[int64][]int64{
	// Unsigned
	255:        {0, 1, 2, 127, 128, 255},
	65535:      {0, 1, 2, 32767, 32768, 65535},
	16777215:   {0, 1, 2, 8388607, 8388608, 16777215},
	4294967295: {0, 1, 2, 2147483647, 2147483648, 4294967295},

	// Signed
	127:                 {-128, -127, -1, 0, 1, 2, 127},
	32767:               {-32768, -32767, -1, 0, 1, 2, 32767},
	8388607:             {-8388608, -8388607, -1, 0, 1, 2, 8388607},
	2147483647:          {-2147483648, -2147483647, -1, 0, 1, 2, 2147483647},
	9223372036854775807: {-9223372036854775808, -9223372036854775807, -1, 0, 1, 2, 9223372036854775807},
}

func Int63nWithSpecialValue(low, high int64) int64 {
	if rand.Intn(10) != 0 {
		if high == 9223372036854775807 {
			return rand.Int63()
		}
		return rand.Int63n(high-low+1) + low
	}
	cases, ok := CornerCaseUnsignedNumMap[high]
	if !ok {
		if rand.Intn(1) == 0 {
			return low
		} else {
			return high
		}
	}
	return cases[rand.Intn(len(cases))]
}

func Int63nWithSpecialValueUBig() uint64 {
	if rand.Intn(10) != 0 {
		return rand.Uint64()
	}
	return UbigInt[rand.Intn(len(UbigInt))]
}

func RandomNum(low, high int64) string {
	num := Int63nWithSpecialValue(low, high)
	return strconv.FormatInt(num+low, 10)
}

func RandomNumsUBig(count int) []string {
	nums := make([]uint64, count)
	for i := 0; i < count; i++ {
		nums[i] = Int63nWithSpecialValueUBig()
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = strconv.FormatUint(nums[i], 10)
	}
	return result
}

func RandomNums(low, high int64, count int) []string {
	nums := make([]int64, count)
	for i := 0; i < count; i++ {
		nums[i] = Int63nWithSpecialValue(low, high)
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = strconv.FormatInt(nums[i], 10)
	}
	return result
}

func RandomFloat(low, high float64) float64 {
	return low + rand.Float64()*(high-low)
}

func RandomBool() bool {
	return rand.Intn(2) == 0
}

func ShouldValid(i int) bool {
	return rand.Intn(100) < i
}
