package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func BenchmarkStringConverter_Float(b *testing.B) {
	conv := buildStringConverter(nil)
	val := 123456.7890123
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conv(val)
	}
}

func BenchmarkStringConverter_String(b *testing.B) {
	conv := buildStringConverter(nil)
	val := "BNPP-CHINA-HQ-OFFICE"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conv(val)
	}
}

func BenchmarkStringConverter_DateTime(b *testing.B) {
	conv := buildStringConverter(nil)
	val := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conv(val)
	}
}

func BenchmarkParquetConverter_Float(b *testing.B) {
	conv := buildConverter(nil)
	val := -1138558.48868431
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conv(val)
	}
}

func BenchmarkParquetConverter_Int(b *testing.B) {
	conv := buildConverter(nil)
	val := int64(1234567890)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conv(val)
	}
}

func BenchmarkTableFormatting_RowGeneration(b *testing.B) {
	colWidths := []int{10, 10, 15, 15, 10}
	isNumericCol := []bool{false, false, true, true, false}
	rowVals := []string{"BNPP-CHINA", "BNPP", "-4278511.58222676", "-4278511.58222676", "2026-09-01"}

	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		rowParts := make([]string, len(rowVals))
		for j, val := range rowVals {
			if isNumericCol[j] {
				rowParts[j] = fmt.Sprintf(" %*s ", colWidths[j], val)
			} else {
				rowParts[j] = fmt.Sprintf(" %-*s ", colWidths[j], val)
			}
		}
		_, _ = fmt.Fprintln(&buf, strings.Join(rowParts, "|"))
	}
}
