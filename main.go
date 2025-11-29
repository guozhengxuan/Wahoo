package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/gitzhang10/BFT/config"
	"github.com/gitzhang10/BFT/gradeddag"
	"github.com/gitzhang10/BFT/tusk"
	"github.com/gitzhang10/BFT/wahoo"
)

var conf *config.Config
var err error

func init() {
	conf, err = config.LoadConfig("", "config")
	if err != nil {
		panic(err)
	}
}

func main() {
	if conf.Protocol == "gradeddag" {
		startGradedDAG()
	} else if conf.Protocol == "tusk" {
		startTusk()
	} else if conf.Protocol == "wahoo" {
		startWahoo()
	} else {
		panic(errors.New("the protocol is unknown"))
	}
}

func startGradedDAG() {
	node := gradeddag.NewNode(conf)
	if err = node.StartP2PListen(); err != nil {
		panic(err)
	}
	// wait for each node to start
	time.Sleep(time.Second * 5)
	if err = node.EstablishP2PConns(); err != nil {
		panic(err)
	}
	node.InitCBC(conf)
	fmt.Println("node starts the GradedDAG!")
	go node.RunLoop()
	go node.HandleMsgLoop()
	go node.CBCOutputBlockLoop()
	node.DoneOutputLoop()

}

func startWahoo() {
	node := wahoo.NewNode(conf)
	if err = node.StartP2PListen(); err != nil {
		panic(err)
	}
	// wait for each node to start
	time.Sleep(time.Second * 15)
	if err = node.EstablishP2PConns(); err != nil {
		panic(err)
	}
	node.InitPB(conf)
	fmt.Println("node starts the Wahoo!")
	go node.RunLoop()
	go node.HandleMsgLoop()
	node.PBOutputBlockLoop()
	node.DoneOutputLoop()
}

func startTusk() {
	node := tusk.NewNode(conf)
	if err = node.StartP2PListen(); err != nil {
		panic(err)
	}
	// wait for each node to start
	time.Sleep(time.Second * 15)
	if err = node.EstablishP2PConns(); err != nil {
		panic(err)
	}
	node.InitRBC(conf)
	fmt.Println("node starts the Tusk!")
	go node.RunLoop()
	go node.HandleMsgLoop()
	node.ConstructedBlockLoop()
}
