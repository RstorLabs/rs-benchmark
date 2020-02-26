package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestInfiniteReader(t *testing.T) {
	sizes := []int{10, 100, 1000, 100000, 1000000, 1000000000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			reader := InfiniteReader{Size: int64(size)}
			out, err := ioutil.ReadAll(&reader)
			require.NoError(t, err)
			require.Equal(t, size, len(out))
		})
	}
}

func BenchmarkInfiniteReader(b *testing.B) {
	sizes := []int{10, 100, 1000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reader := InfiniteReader{Size: int64(size)}
				out, err := ioutil.ReadAll(&reader)
				require.NoError(b, err)
				require.Equal(b, size, len(out))
			}
		})
	}
}
