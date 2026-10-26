package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

const (
	defaultPort  = 9000
	replyTimeout = time.Second
	retryDelay   = 200 * time.Millisecond
	logInterval  = 3 * time.Second
)

type node struct {
	port int
	sock mangos.Socket

	mu        sync.Mutex
	neighbors []int
}

func url(port int) string {
	return fmt.Sprintf("tcp://127.0.0.1:%d", port)
}

func listen(port int) (mangos.Socket, error) {
	sock, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sock.Listen(url(port)); err != nil {
		sock.Close()
		return nil, err
	}
	return sock, nil
}

func request(port int, msg string) (string, error) {
	sock, err := req.NewSocket()
	if err != nil {
		return "", err
	}
	defer sock.Close()
	sock.SetOption(mangos.OptionSendDeadline, replyTimeout)
	sock.SetOption(mangos.OptionRecvDeadline, replyTimeout)

	if err := sock.Dial(url(port)); err != nil {
		return "", err
	}
	if err := sock.Send([]byte(msg)); err != nil {
		return "", err
	}
	reply, err := sock.Recv()
	if err != nil {
		return "", err
	}
	return string(reply), nil
}

// list of integer port into formatted string port
func formatPorts(ports []int) string {
	str := make([]string, len(ports))
	for i, port := range ports {
		str[i] = strconv.Itoa(port)
	}
	return strings.Join(str, ",")
}

// reverse of the previous function :P
func parsePorts(str string) ([]int, error) {
	if str == "" {
		return nil, errors.New("empty port list")
	}
	var ports []int
	for _, f := range strings.Split(str, ",") {
		p, err := strconv.Atoi(f)
		if err != nil {
			return nil, err
		}
		ports = append(ports, p)
	}
	return ports, nil
}

func (nodeInfo *node) knownPorts() []int {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	ports := append([]int{nodeInfo.port}, nodeInfo.neighbors...)
	slices.Sort(ports)
	return ports
}

func (nodeInfo *node) neighborList() []int {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	return slices.Sorted(slices.Values(nodeInfo.neighbors))
}

func (nodeInfo *node) addNeighbor(port int) {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	if !slices.Contains(nodeInfo.neighbors, port) {
		nodeInfo.neighbors = append(nodeInfo.neighbors, port)
	}
}

// serve answers requests from other nodes:
//	PORTS		every port this node knows (itself included)
//	JOIN <port> add port to neighbors
func (nodeInfo *node) serve() {
	for {
		msg, err := nodeInfo.sock.Recv()
		if errors.Is(err, mangos.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("[%d] recv: %v", nodeInfo.port, err)
			continue
		}

		var reply string
		cmd, arg, _ := strings.Cut(string(msg), " ")
		switch cmd {
		case "PORTS":
			reply = formatPorts(nodeInfo.knownPorts())
		case "JOIN":
			p, err := strconv.Atoi(arg)
			if err != nil {
				reply = "ERR bad port"
				break
			}
			nodeInfo.addNeighbor(p)
			reply = "OK"
		default:
			reply = "ERR unknown command"
		}

		if err := nodeInfo.sock.Send([]byte(reply)); err != nil {
			log.Printf("[%d] send: %v", nodeInfo.port, err)
		}
	}
}

func main() {
	nodeInfo := &node{} // list of nodes

	// port selection
	for nodeInfo.sock == nil {
		reply, err := request(defaultPort, "PORTS")

		if err != nil {
			// if default port inactive -> serve default port -> if failed, retry if port active again and get response again(maybe another machine is starting)
			if sock, err := listen(defaultPort); err == nil {
				nodeInfo.port, nodeInfo.sock = defaultPort, sock
			}
		} else if ports, err := parsePorts(reply); err == nil {
			port := slices.Max(ports) + 1

			// if there isnt -> get the highest port and increment, use that as current port -> if failed retry
			if sock, err := listen(port); err == nil {
				nodeInfo.port, nodeInfo.sock = port, sock
				nodeInfo.neighbors = ports
			}
		}

		if nodeInfo.sock == nil {
			time.Sleep(retryDelay)
		}
	}

	// file logging stuff
	logFile, err := os.Create(fmt.Sprintf("logs-%d.log", nodeInfo.port))
	if err != nil {
		log.Fatalf("[%d] log file: %v", nodeInfo.port, err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stderr, logFile))

	log.Printf("[%d] up, neighbors %v", nodeInfo.port, nodeInfo.neighbors)
	go nodeInfo.serve()

	if nodeInfo.port != defaultPort {
		if _, err := request(defaultPort, "JOIN "+strconv.Itoa(nodeInfo.port)); err != nil {
			log.Printf("[%d] join: %v", nodeInfo.port, err)
		}
	}

	for range time.Tick(logInterval) {
		log.Printf("[%d] neighbors %v", nodeInfo.port, nodeInfo.neighborList())
	}
}
