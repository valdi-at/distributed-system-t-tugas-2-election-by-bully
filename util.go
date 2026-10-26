package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
)

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
