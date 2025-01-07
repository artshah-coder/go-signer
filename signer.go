package main

import (
	"sort"
	"strconv"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	channels := make([]chan interface{}, len(jobs)-1)
	var wg sync.WaitGroup
	for i := 0; i < len(jobs)-1; i++ {
		channels[i] = make(chan interface{})
		if i == 0 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				jobs[i](make(chan interface{}), channels[i])
				close(channels[i])
			}(i)
		} else {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				jobs[i](channels[i-1], channels[i])
				close(channels[i])
			}(i)
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		jobs[len(jobs)-1](channels[len(channels)-1], make(chan interface{}))
	}()
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	for data := range in {
		md5 := DataSignerMd5(strconv.Itoa(data.(int)))
		wg.Add(1)
		go func(data interface{}, md5 string) {
			defer wg.Done()
			var w sync.WaitGroup
			crc32_data, crc32_md5 := "", ""
			w.Add(2)
			go func() {
				defer w.Done()
				crc32_data = DataSignerCrc32(strconv.Itoa(data.(int)))
			}()
			go func() {
				defer w.Done()
				crc32_md5 = DataSignerCrc32(md5)
			}()
			w.Wait()
			out <- crc32_data + "~" + crc32_md5
		}(data, md5)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wg, w sync.WaitGroup
	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			defer wg.Done()
			multiHash := ""
			slice := make([]string, 6)
			for i := 0; i < 6; i++ {
				w.Add(1)
				go func(data interface{}, i int) {
					defer w.Done()
					slice[i] = DataSignerCrc32(strconv.Itoa(i) + data.(string))
				}(data, i)
			}
			w.Wait()
			for _, hash := range slice {
				multiHash += hash
			}
			out <- multiHash
		}(data)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	accum := []string{}
	for data := range in {
		accum = append(accum, data.(string))
	}
	sort.Strings(accum)
	result := ""
	for i, s := range accum {
		result += s
		if i != len(accum)-1 {
			result += "_"
		}
	}
	out <- result
}
