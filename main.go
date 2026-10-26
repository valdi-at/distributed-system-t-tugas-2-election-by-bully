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
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

const (
	replyTimeout      = time.Second
	retryDelay        = 200 * time.Millisecond
	logInterval       = 3 * time.Second
	heartbeatInterval = time.Second
	heartbeatTTL      = 3 * time.Second // no ping for this long -> coordinator drops the node
	electionTimeout   = 3 * time.Second // got OK from a higher node but no ELECTION_COORD for this long -> run election again
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
	electing    bool              // an election is running right now, dont start another one
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
	nodeInfo.neighbors = slices.DeleteFunc(nodeInfo.neighbors, func(p int) bool { return p == port })
	delete(nodeInfo.lastSeen, port)
	return true
}

// make ourself the coordinator, lastSeen starts fresh so nobody gets dropped because of old timestamps
func (nodeInfo *node) claimCoordinator() {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	nodeInfo.coordinator = nodeInfo.port
	clear(nodeInfo.lastSeen)
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
//	ELECTION_ASK <port>		reply OK and start our own election (port is who asked)
//	ELECTION_COORD <port>	port won the election, set it as our coordinator
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
		case "ELECTION_ASK":
			log.Printf("[%d] got ELECTION_ASK from %s", nodeInfo.port, arg)
			reply = "OK"
			nodeInfo.startElection()
		case "ELECTION_COORD":
			p, err := strconv.Atoi(arg)
			if err != nil {
				reply = "ERR bad port"
				break
			}
			nodeInfo.addNeighbor(p)
			if nodeInfo.getCoordinator() != p {
				log.Printf("[%d] %d won the election, its the coordinator now", nodeInfo.port, p)
			}
			nodeInfo.setCoordinator(p)
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
		if coord == nodeInfo.port {
			continue // itsa me! the coordinator
		}
		if coord == 0 {
			// no coordinator (we joined in the middle of an election or something), go find one
			nodeInfo.startElection()
			continue
		}
		// no PONG -> coordinator is ded, go to no coordinator and start an election
		reply, err := request(coord, "PING "+strconv.Itoa(nodeInfo.port))
		if err == nil && reply != "PONG" {
			err = fmt.Errorf("weird reply %q", reply)
		}
		if err != nil && nodeInfo.clearCoordinator(coord) {
			log.Printf("[%d] coordinator %d didnt answer ping (%v), no coordinator now", nodeInfo.port, coord, err)
			nodeInfo.startElection()
		}
	}
}

// startElection runs election in the background, unless one is already running on this node
func (nodeInfo *node) startElection() {
	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()
	if nodeInfo.electing {
		return
	}
	nodeInfo.electing = true

	go func() {
		nodeInfo.election()
		nodeInfo.mu.Lock()
		nodeInfo.electing = false
		nodeInfo.mu.Unlock()
	}()
}

// bully election: send ELECTION_ASK to every higher port
// nobody answers OK -> we are the highest one alive, become coordinator and send ELECTION_COORD to everyone
// someone answers OK -> they run their own election, we wait for their ELECTION_COORD
// (if it doesnt come within electionTimeout, the higher one probably died too, so go again)
func (nodeInfo *node) election() {
	for {
		nodeInfo.setCoordinator(0) // were picking a new one
		log.Printf("[%d] starting election", nodeInfo.port)

		higherAlive := false
		for _, port := range nodeInfo.neighborList() {
			if port <= nodeInfo.port {
				continue
			}
			if reply, err := request(port, "ELECTION_ASK "+strconv.Itoa(nodeInfo.port)); err == nil && reply == "OK" {
				higherAlive = true
			}
		}

		if !higherAlive {
			nodeInfo.claimCoordinator()
			log.Printf("[%d] nobody higher answered, im the coordinator now", nodeInfo.port)
			for _, port := range nodeInfo.neighborList() {
				if _, err := request(port, "ELECTION_COORD "+strconv.Itoa(nodeInfo.port)); err != nil {
					log.Printf("[%d] ELECTION_COORD to %d: %v", nodeInfo.port, port, err)
				}
			}
			return
		}

		deadline := time.Now().Add(electionTimeout)
		for time.Now().Before(deadline) && nodeInfo.getCoordinator() == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		if nodeInfo.getCoordinator() != 0 {
			return
		}
		log.Printf("[%d] got OK but nobody sent ELECTION_COORD in %v, trying again", nodeInfo.port, electionTimeout)
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
