package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	//writeData()
	readData()
}

func writeData() {
	file, err := os.OpenFile("./.csv_files/file_1.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString("1,2,3,4,5,")
	if err != nil {
		panic(err)
	}

	_ = writer.Flush()
	_ = file.Close()
}

func readData() {
	file, err := os.OpenFile("./.csv_files/file_1.csv", os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}

	reader := bufio.NewScanner(file)
	reader.Scan()
	data := reader.Bytes()
	ids := strings.Split(string(data), `,`)
	fmt.Println(ids)
}
