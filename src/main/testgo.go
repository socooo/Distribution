package main

import (
	"fmt"
	"time"
	"sync"
)

type teststr struct {
	mu sync.Mutex
	num int
}
func main(){
	//s := test()
	//println(s)
	//time.Sleep(4*time.Second)
	//var a []int
	//a = append(a, 0)
	//a = append(a, 1)
	//a = append(a, 2)
	//a = append(a, 3)
	//a = append(a, 4)
	//a = append(a, 5)
	//
	//fmt.Printf("a: %v.\n", a[len(a)-1])
	//a = a[0:0]
	//fmt.Printf("a after clear: %v.\n", a)

	//fileName := "debug_info.log"
	//file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE, 0777)
	//if err != nil {
	//	log.Fatal("Can not create log file, " + err.Error())
	//}
	//debugLog := log.New(file, "aa ", log.Llongfile)
	//debugLog.Printf("this file name")
	//file.Close()

	//chanone := make(chan int)
	//chantwo := make(chan int)
	//chanthree := make(chan int)
	//
	//testmap := make(map[int]chan int)
	//testmap[2] = chanone
	//testmap[2] = chantwo
	//testmap[3] = chanthree
	//fmt.Printf("test map: %v\n", testmap)
	//delete(testmap,2)
	//delete(testmap, 3)
	//fmt.Printf("test map: %v\n", testmap)

	//teststra := teststr{num : 1}
	//
	//for i:=0; i<3; i++{
	//	go func(no int){
	//		fmt.Printf("first this is: %v.\n", no)
	//		teststra.mu.Lock()
	//		fmt.Printf("a: %v.\n", teststra.num)
	//		time.Sleep(5*time.Second)
	//		teststra.mu.Unlock()
	//		fmt.Printf("last this is: %v.\n", no)
	//	}(i)
	//}
	//time.Sleep(20*time.Second)

	//testTimer := time.NewTimer(2*time.Second)
	//go func(){
	//	timeStart := time.Now()
	//	time.Sleep(3000*time.Millisecond)
	//	testTimer.Reset(3*time.Second)
	//	select {
	//	case <- testTimer.C:
	//		timeEnd := time.Now()
	//		fmt.Printf("time eclipse: %v.\n", timeEnd.Second() - timeStart.Second())
	//	}
	//	testTimer.Reset(4*time.Second)
	//	testTimer.Stop()
	//	testTimer.Reset(6*time.Second)
	//
	//	select {
	//	case <- testTimer.C:
	//		timeEnd := time.Now()
	//		fmt.Printf("time eclipse: %v.\n", timeEnd.Second() - timeStart.Second())
	//	}
	//}()
	//time.Sleep(12*time.Second)

	a:=2
	switch a {
	case 1: println(1)
	case 2: println(2)
		break
	case 3: println(3)
	default:
		println("no such number.")
	}
}

func test() string{
	doneChan := make(chan bool)
	go goroutineTest(doneChan)
	time.Sleep(6*time.Second)
	println("in test sleep time up.")
	return "test"
}
func goroutineTest(doneChan chan bool){
	println("in goroutine.")
	heartBeatTimer := time.NewTicker(1*time.Second)
	secondTimer := time.NewTicker(1200 * time.Millisecond)
	i:=0
	for{
		select {
		case <- heartBeatTimer.C:
			i+=1
			fmt.Printf("this is %v\n" , i)
			if i==3 {
				return
			}
		case <- secondTimer.C:
			fmt.Printf("second timer.\n")
		default:
			time.Sleep(800 * time.Millisecond)
			fmt.Printf("default.\n")
		}

	}

}
