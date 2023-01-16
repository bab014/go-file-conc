package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var nw int

func init() {
	flag.IntVar(&nw, "workers", 1, "Number of workers")
	flag.Parse()
}

func main() {
	start := time.Now()

	ch := make(chan []string)
	files := [4]string{"file1.csv", "file2.csv", "file3.csv", "file4.csv"}
	var wg sync.WaitGroup
	wg.Add(len(files))

	for idx, file := range files {
		go processFile(idx, file, &wg, ch)
	}

	// closer
	go func(wg *sync.WaitGroup, ch chan []string) {
		fmt.Println("Waiting for file goroutines to finish")
		wg.Wait()
		// not making it here
		fmt.Println("All file goroutines finished")
		close(ch)
	}(&wg, ch)

	// print channel results
	processors := 200
	var wg2 sync.WaitGroup
	wg2.Add(processors)
	counter := make(chan int)
	for i := 0; i < processors; i++ {
		go func(w int, wg *sync.WaitGroup, raw <-chan []string, c chan<- int) {
			defer wg.Done()
			var j int
			for range raw {
				// process records sent from files
				j++
				fmt.Printf("%d processed %d\n", w, j)
			}
			c <- j
		}(i, &wg2, ch, counter)
	}
	go func() {
		wg2.Wait()
		fmt.Println("All processor goroutines finished")
		close(counter)
	}()
	var records int
	for count := range counter {
		records += count
	}

	log.Printf("Time since start: %s, processed records %d", time.Since(start), records)
}

//	func processData(i int, r []string) {
//		// process a record
//		time.Sleep(10 * time.Millisecond)
//		fmt.Println("Processing record ", i)
//	}
func processFile(idx int, file string, wg *sync.WaitGroup, ch chan<- []string) {
	defer wg.Done()
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	csvR := csv.NewReader(f)
	for {
		r, err := csvR.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF", idx)
				// end of file
				break
			}
			log.Fatal(err)
		}
		if !(strings.TrimSpace(r[1]) == "") {
			ch <- r
		}
	}
	fmt.Println("Done processing file ", idx+1)
}

func processRecord(w int, ch <-chan []string, wg *sync.WaitGroup) {
	defer wg.Done()
	// process a record
	for range ch {
	}
}
