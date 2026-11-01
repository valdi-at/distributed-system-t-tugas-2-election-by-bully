package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/respondent"
	"go.nanomsg.org/mangos/v3/protocol/surveyor"
)

var errNoReply = errors.New("no reply")

func url(port int) string {
	return fmt.Sprintf("tcp://127.0.0.1:%d", port)
}

func listen(port int) (mangos.Socket, error) {
	sock, err := respondent.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sock.Listen(url(port)); err != nil {
		sock.Close()
		return nil, err
	}
	return sock, nil
}

// survey sends a msg to all port and get all of the replies until all of them answered or replyTimeout ran out
// dead ports are skipped, err is only set when none of the ports responds
func survey(ports []int, msg string) ([]string, error) {
	sock, err := surveyor.NewSocket()
	if err != nil {
		return nil, err
	}
	defer sock.Close()
	sock.SetOption(mangos.OptionSurveyTime, replyTimeout)

	dialed := 0
	err = errNoReply
	for _, port := range ports {
		if dialErr := sock.Dial(url(port)); dialErr != nil {
			err = dialErr
			continue
		}
		dialed++
	}
	if dialed == 0 {
		return nil, err
	}
	if err := sock.Send([]byte(msg)); err != nil {
		return nil, err
	}

	var replies []string
	for len(replies) < dialed {
		reply, err := sock.Recv()
		if err != nil {
			break // ErrProtoState = survey expired
		}
		replies = append(replies, string(reply))
	}
	return replies, nil
}

// request is a survey to a single port, basically like req/rep with a deadline :P
func request(port int, msg string) (string, error) {
	replies, err := survey([]int{port}, msg)
	if err != nil {
		return "", err
	}
	if len(replies) == 0 {
		return "", errNoReply
	}
	return replies[0], nil
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
