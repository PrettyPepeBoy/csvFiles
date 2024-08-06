package main

import (
	"crypto/rand"
	"encoding/csv"
	"math/big"
	"os"
	"strconv"
)

func main() {
	for i := 1; i <= 1000; i++ {
		filename := "file_" + strconv.Itoa(i) + ".csv"
		f, err := os.OpenFile("./.csv_files/"+filename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		writer := csv.NewWriter(f)

		err = writer.Write(genData())
		if err != nil {
			panic(err)
		}

		writer.Flush()
		_ = f.Close()
	}
}

func genData() []string {
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(1000)))
	if err != nil {
		panic(err)
	}

	slc := make([]string, int(nBig.Int64()))

	for i := 0; i < len(slc); i++ {
		nBig, err = rand.Int(rand.Reader, big.NewInt(int64(999_999)))
		if err != nil {
			panic(err)
		}

		slc[i] = strconv.Itoa(int(nBig.Int64()))
	}

	return slc
}
