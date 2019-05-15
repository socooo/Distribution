package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	fileName := "debug_info.log"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal("Can not create log file, " + err.Error())
	}
	debugLog := log.New(file, "", log.Llongfile)
	if Debug > 0 {
		debugLog.Printf(format, a...)
	}
	return
}
