package proxy

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"testing"
)

func TestMyCopyN(t *testing.T) {

	a := assert.New(t)

	buf1 := make([]byte, 1)
	buf2 := make([]byte, 2)
	buf16 := make([]byte, 16)
	buf4096 := make([]byte, 4096)

	tests := []struct {
		buf     []byte
		strSize int
	}{
		{buf1, 0},
		{buf1, 1},
		{buf1, 2},
		{buf1, 4},
		{buf1, 16},

		{buf2, 0},
		{buf2, 1},
		{buf2, 2},
		{buf2, 4},
		{buf2, 16},

		{buf16, 0},
		{buf16, 1},
		{buf16, 2},
		{buf16, 16},
		{buf16, 32},
		{buf16, 1111},

		{buf4096, 0},
		{buf4096, 1},
		{buf4096, 2},
		{buf4096, 4095},
		{buf4096, 4096},
		{buf4096, 4097},
		{buf4096, 8191},
		{buf4096, 8192},
		{buf4096, 8193},
		{buf4096, 16383},
		{buf4096, 16384},
		{buf4096, 16385},
	}
	for _, tt := range tests {

		rb := new(bytes.Buffer)
		wb := new(bytes.Buffer)
		text := randomString(tt.strSize)

		rb.WriteString(text)
		_, err := myCopyN(wb, rb, int64(tt.strSize), tt.buf)
		a.Nil(err)
		a.Equal(text, wb.String())

		// extra bytes in buffer
		rb.Reset()
		wb.Reset()
		rb.WriteString(text)
		rb.WriteString(randomString(11))
		_, err = myCopyN(wb, rb, int64(tt.strSize), tt.buf)
		a.Nil(err)
		a.Equal(text, wb.String())

		// EOF
		rb.Reset()
		wb.Reset()
		rb.WriteString(text)
		readErr, err := myCopyN(wb, rb, int64(tt.strSize+1), tt.buf)
		a.True(readErr)
		a.Equal(io.EOF, err)
	}

}

func randomString(n int) string {
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}
