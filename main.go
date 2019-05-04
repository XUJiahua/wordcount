// 通过channel来串联起来

// 工作与工作之间通过channel来协作。

// 1、遍历文件夹，所有文件塞入chan1，由mapper来消费。
// 2、mapper函数，指定并行10个，消费chan1。每个mapper的结果存入chan2.
// 3、reducer函数，消费chan2，再次聚合存储到chan3
// 4、main函数取chan3的信息。

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
)

const (
	MaxMapperNum = 3
)

type MapRes map[string]int

func enumerateFiles(dir string) chan string {
	fileChan := make(chan string)

	go func() {
		fmt.Println("INFO: file walker goroutine start")

		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				fileChan <- path
			}
			return nil
		})

		close(fileChan)

		fmt.Println("INFO: file walker goroutine end")
	}()

	return fileChan
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func mapper(filename string) MapRes {
	file, err := os.Open(filename)
	panicOnErr(err)
	defer file.Close()

	output := make(MapRes)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// TODO: 分词还需要考虑标点符号
		words := strings.Fields(scanner.Text())
		for _, word := range words {
			output[word]++
		}
	}
	// spew.Dump(output)

	panicOnErr(scanner.Err())
	return output
}

func reducer(mapperChan chan MapRes) chan MapRes {
	reducerChan := make(chan MapRes)
	go func() {
		output := make(MapRes)
		fmt.Println("INFO: reducer goroutine start")
		for mapRes := range mapperChan {
			for k, v := range mapRes {
				// if lowercase
				// output[k] += v
				output[strings.ToLower(k)] += v
			}
		}
		// TODO: sort top10
		reducerChan <- output
		close(reducerChan)
		fmt.Println("INFO: reducer goroutine end")
	}()
	return reducerChan
}

func mapperDispatcher(fileChan chan string) chan MapRes {
	mapperChan := make(chan MapRes, MaxMapperNum)

	go func() {
		fmt.Println("INFO: mapper dispatcher goroutine start")

		wg := &sync.WaitGroup{}

		for file := range fileChan {
			wg.Add(1)
			// in parallel
			go func(filename string) {
				fmt.Println("INFO: mapper goroutine start")
				fmt.Println("processing", filename)
				mapperChan <- mapper(filename)
				wg.Done()
				fmt.Println("INFO: mapper goroutine end")
			}(file)
		}

		wg.Wait()
		close(mapperChan)

		fmt.Println("INFO: mapper dispatcher goroutine end")
	}()

	return mapperChan
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("./wordcount folder \n")
		return
	}

	fmt.Println("INFO: main goroutine start")
	dir := os.Args[1]

	fileChan := enumerateFiles(dir)
	mapperChan := mapperDispatcher(fileChan)
	reducerChan := reducer(mapperChan)

	res := <-reducerChan
	spew.Dump(res)

	f, err := os.Create("words.txt")
	panicOnErr(err)
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()
	for k, v := range res {
		_, err = fmt.Fprintf(w, "%s,%d\n", k, v)
		panicOnErr(err)
	}
	fmt.Println("INFO: main goroutine end")
}
