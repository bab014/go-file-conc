package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	COUNTRY_COL int = iota
	CODE_COL
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
	proc := runtime.NumCPU()
	var wg2 sync.WaitGroup
	wg2.Add(proc)
	output := make(chan []string)
	for i := 0; i < proc; i++ {
		go func(w int, wg *sync.WaitGroup, raw <-chan []string, o chan<- []string) {
			defer wg.Done()
			for rec := range raw {
				// process records sent from files
				o <- rec
			}
		}(i, &wg2, ch, output)
	}
	go func() {
		wg2.Wait()
		fmt.Println("All processor goroutines finished")
		close(output)
	}()

	var wg3 sync.WaitGroup
	outF, err := os.Create("output.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer outF.Close()
	w := csv.NewWriter(outF)
	wg3.Add(1)
	go writeToFile(&wg3, output, w)
	wg3.Wait()

	log.Printf("Time since start: %s, processed records", time.Since(start))
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
		if !(strings.TrimSpace(r[CODE_COL]) == "") {
			ch <- r
		}
	}
	fmt.Println("Done processing file ", idx+1)
}

// the final function of the pipeline that writes records to a file
// if the record for that country has not already been written
func writeToFile(wg *sync.WaitGroup, ch <-chan []string, w *csv.Writer) {
	defer wg.Done()
	records := make(map[string]struct{})
	var j int
	for record := range ch {
		j++
		if uniqueRecords(records, record) {
			writeRecord(w, record)
		}
	}
	log.Printf("Final processor did %d records", j)
	w.Flush()
}

// maintain unique set of records based on country
func uniqueRecords(records map[string]struct{}, record []string) bool {
	_, ok := records[record[COUNTRY_COL]]
	if !ok {
		records[record[COUNTRY_COL]] = struct{}{}
		return true
	}
	return false
}

// write record
func writeRecord(w *csv.Writer, record []string) {
	if err := w.Write(record); err != nil {
		log.Fatal(err)
	}
}
