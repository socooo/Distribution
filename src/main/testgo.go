package main

import (
	"fmt"
	"time"
)

func main(){
	//s := test()
	//println(s)
	//time.Sleep(4*time.Second)
	a:=5/2
	fmt.Printf("a: %v", a)
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
