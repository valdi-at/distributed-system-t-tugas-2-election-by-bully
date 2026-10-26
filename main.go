package main

import (
	"errors"
	"flag"
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
	replyTimeout      = time.Second
	retryDelay        = 200 * time.Millisecond
	logInterval       = 3 * time.Second
	heartbeatInterval = time.Second
	heartbeatTTL      = 3 * time.Second // no ping for this long -> coordinator drops the node
)

// cluster port range defaultPort = start, lastPort = end, inclusive
var defaultPort, lastPort int

type node struct {
	port int
	sock mangos.Socket

	mu          sync.Mutex
	neighbors   []int
	coordinator int               // 0 = dunno
	lastSeen    map[int]time.Time // last ping from each neighbor, only used by the coordinator
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

// e.g. "9000-9100" into 9000, 9100
func parseRange(str string) (int, int, error) {
	a, b, ok := strings.Cut(str, "-")
	if !ok {
		return 0, 0, errors.New("should look like 9000-9100")
	}
	first, err := strconv.Atoi(a)
	if err != nil {
		return 0, 0, err
	}
	last, err := strconv.Atoi(b)
	if err != nil {
		return 0, 0, err
	}
	if first < 1 || last > 65535 || first >= last {
		return 0, 0, errors.New("need 1 <= first < last <= 65535")
	}
	return first, last, nil
}

// search nodes in the port range one by one and returns the port list of the first node that answers
func findCluster() ([]int, bool) {
	for port := defaultPort + 1; port <= lastPort; port++ {
		reply, err := request(port, "PORTS")
		if err != nil {
			continue
		}
		ports, err := parsePorts(reply)
		if err != nil {
			continue
		}
		return slices.DeleteFunc(ports, func(p int) bool { return p == defaultPort }), true
	}
	return nil, false
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

func (nodeInfo *node) getCoordinator() int {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	return nodeInfo.coordinator
}

func (nodeInfo *node) setCoordinator(port int) {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	nodeInfo.coordinator = port
}

// set coordinator to 0
func (nodeInfo *node) clearCoordinator(port int) bool {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	if nodeInfo.coordinator != port {
		return false
	}
	nodeInfo.coordinator = 0
	return true
}

func (nodeInfo *node) addNeighbor(port int) {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	if !slices.Contains(nodeInfo.neighbors, port) {
		nodeInfo.neighbors = append(nodeInfo.neighbors, port)
	}
}

func (nodeInfo *node) removeNeighbor(port int) {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	nodeInfo.neighbors = slices.DeleteFunc(nodeInfo.neighbors, func(p int) bool { return p == port })
	delete(nodeInfo.lastSeen, port)
}

// touch records a ping from port (ignored if its not a neighbor)
func (nodeInfo *node) touch(port int) {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	if slices.Contains(nodeInfo.neighbors, port) {
		nodeInfo.lastSeen[port] = time.Now()
	}
}

// removes all neighbors that havent pinged within heartbeatTTL, returns the lazy ahh neighbors
func (nodeInfo *node) dropExpired() []int {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()

	now := time.Now()
	var expired []int
	for _, port := range nodeInfo.neighbors {
		seen, ok := nodeInfo.lastSeen[port]
		if !ok {
			// new node or we just become coordinator
			nodeInfo.lastSeen[port] = now
			continue
		}
		if now.Sub(seen) > heartbeatTTL {
			expired = append(expired, port)
		}
	}

	nodeInfo.neighbors = slices.DeleteFunc(nodeInfo.neighbors, func(p int) bool { return slices.Contains(expired, p) })
	for _, port := range expired {
		delete(nodeInfo.lastSeen, port)
	}
	return expired
}

// serve answers requests from other nodes:
//	PORTS		every port this node knows (itself included)
//	JOIN <port> add port to neighbors, reply with our coordinator (0 if we dont know yet)
//	PING <port> heartbeat from port, the coordinator uses it for the ttl
//	DROP <port> remove port from neighbors (sent by the coordinator)
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
			reply = strconv.Itoa(nodeInfo.getCoordinator())
		case "PING":
			if p, err := strconv.Atoi(arg); err == nil {
				nodeInfo.touch(p)
			}
			reply = "PONG"
		case "DROP":
			p, err := strconv.Atoi(arg)
			if err != nil {
				reply = "ERR bad port"
				break
			}
			nodeInfo.removeNeighbor(p)
			log.Printf("[%d] coordinator says %d is gone, dropped it", nodeInfo.port, p)
			reply = "OK"
		default:
			reply = "ERR unknown command"
		}

		if err := nodeInfo.sock.Send([]byte(reply)); err != nil {
			log.Printf("[%d] send: %v", nodeInfo.port, err)
		}
	}
}

// ping coordinator every sec
func (nodeInfo *node) heartbeat() {
	for range time.Tick(heartbeatInterval) {
		coord := nodeInfo.getCoordinator()
		if coord == 0 || coord == nodeInfo.port {
			continue // dont know it yet, or it could be ourself
		}
		// no PONG -> coordinator is ded, go to no coordinator (no election yet)
		reply, err := request(coord, "PING "+strconv.Itoa(nodeInfo.port))
		if err == nil && reply != "PONG" {
			err = fmt.Errorf("weird reply %q", reply)
		}
		if err != nil && nodeInfo.clearCoordinator(coord) {
			log.Printf("[%d] coordinator %d didnt answer ping (%v), no coordinator now", nodeInfo.port, coord, err)
		}
	}
}

//  coordinator only, drop lazy ahh neighbors and tell others to do the same
func (nodeInfo *node) watchNeighbors() {
	for range time.Tick(heartbeatInterval) {
		if nodeInfo.getCoordinator() != nodeInfo.port {
			continue
		}
		for _, dead := range nodeInfo.dropExpired() {
			log.Printf("[%d] no ping from %d for %v, dropping it", nodeInfo.port, dead, heartbeatTTL)
			for _, port := range nodeInfo.neighborList() {
				if _, err := request(port, "DROP "+strconv.Itoa(dead)); err != nil {
					log.Printf("[%d] drop %d at %d: %v", nodeInfo.port, dead, port, err)
				}
			}
		}
	}
}

func main() {
	portRange := flag.String("range", "9000-9100", "cluster port range, first-last (first one is the default port)")
	flag.Parse()
	first, last, err := parseRange(*portRange)
	if err != nil {
		log.Fatalf("bad -range %q: %v", *portRange, err)
	}
	defaultPort, lastPort = first, last

	nodeInfo := &node{lastSeen: map[int]time.Time{}} // list of nodes

	// port selection
	for nodeInfo.sock == nil {
		reply, err := request(defaultPort, "PORTS")

		if err != nil {
			// if default port inactive -> serve default port -> if failed, retry if port active again and get response again(maybe another machine is starting)
			if sock, err := listen(defaultPort); err == nil {
				nodeInfo.port, nodeInfo.sock = defaultPort, sock
				// default port being free doesnt mean we are first, maybe the old one died and the rest are still up
				if ports, found := findCluster(); found {
					nodeInfo.neighbors = ports // we join them below like any other node, coordinator comes from them
				} else {
					nodeInfo.coordinator = defaultPort // nobody else is up, we are the first
				}
			}
		} else if ports, err := parsePorts(reply); err == nil {
			port := slices.Max(ports) + 1
			if port > lastPort {
				log.Fatalf("no free port left in %d-%d", defaultPort, lastPort)
			}

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

	// tell every node we exist, they reply with their coordinator
	for _, port := range nodeInfo.neighborList() {
		reply, err := request(port, "JOIN "+strconv.Itoa(nodeInfo.port))
		if err != nil {
			log.Printf("[%d] join %d: %v", nodeInfo.port, port, err)
			continue
		}
		// take the first one that knows
		if coord, err := strconv.Atoi(reply); err == nil && coord != 0 && nodeInfo.getCoordinator() == 0 {
			nodeInfo.setCoordinator(coord)
		}
	}
	log.Printf("[%d] coordinator is %d", nodeInfo.port, nodeInfo.getCoordinator())
	go nodeInfo.heartbeat()
	go nodeInfo.watchNeighbors()

	for range time.Tick(logInterval) {
		log.Printf("[%d] coordinator %d, neighbors %v", nodeInfo.port, nodeInfo.getCoordinator(), nodeInfo.neighborList())
	}
}
