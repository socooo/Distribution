package main

import (
	"fmt"
	"time"
	"os"
	"log"
)

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

	fileName := "debug_info.log"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal("Can not create log file, " + err.Error())
	}
	debugLog := log.New(file, "aa ", log.Llongfile)
	debugLog.Printf("this file name")
	file.Close()
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
