package main

import (
	"fmt"
	"os"
)

func main() {

	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please give one argument.")
		return
	}

	path := arguments[1]

	fileInfo, err := os.Stat(path)
	if err != nil {
		fmt.Println("Path does not exist!", err)
	}

	mode := fileInfo.Mode()
	switch mode.IsDir() {
	case true:
		fmt.Println(path, "is a directory!")
	case false:
		fmt.Println(path, "is not a directory - and not a valid directory name!")
	}
}
