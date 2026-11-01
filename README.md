# Tugas 2 - Election by Bullying

| Name                        | NRP        |
| --------------------------- | ---------- |
| Mohamad Valdi Ananda Tauhid | 5025221238 |

## General Structure

Each node is its own application using the same binary.
Each node listens on a port automatically generated when launching
(within the port address bounds).

There is 2 implementation of how nodes talk to each other, the logic (connect, ping, election) is the same,
only the socket pattern is differnt. When a section below is differnt between the two, it has a part for each one.

#### REQ/REP version

Each node listens on its port with a REP socket.
To talk to another node it opens a REQ socket, dials that ONE port, sends the message and waits for the reply
(with a send/recv deadline so a dead node doesnt hang it forever).
Talking to many nodes means looping over them and doing one request each.

#### SURVEYOR/RESPONDENT version

Each node listens on its port with a respondent socket,
and to talk to other nodes it opens a surveyor, dials all the ports it needs, sends the message once
and collects replies until everyone answered or the timeout runs out.
A message to a single node is just a survey with one port (works like req/rep with a deadline).

### REQ/REP vs SURVEYOR/RESPONDENT

Both are "ask something, get an answer back" patterns, the difference is how many nodes get the question
and how many answers come back.

|                           | REQ / REP                                                                                               | SURVEYOR / RESPONDENT                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| who gets the message      | ONE peer. If the REQ socket is dialed to many REPs it picks one (load balancing), it does NOT broadcast | every connected respondent gets a copy                                                                               |
| how many replies          | exactly one                                                                                             | zero or more, you call `Recv()` once per reply                                                                       |
| when does it stop waiting | never by default, you need `OptionRecvDeadline`                                                         | when the survey expires (`OptionSurveyTime`, default 1s), after that `Recv()` fails and late replies get thrown away |
| retry                     | yes, resends the request after `OptionRetryTime` (default 1 minute) or when the peer connection drops   | no retry, YOLO                                                                                                       |
| order rules               | REQ must Send then Recv, REP must Recv then Send                                                        | same idea, but sending a new survey cancels the old one                                                              |
| nanomsg says its for      | RPC, "I ask, you answer"                                                                                | service discovery and voting                                                                                         |

REQ/REP is the only pattern in nanomsg with automatic retry, so its the reliable one when u need an answer from one specific service.
The catch is it only ever talks to one peer per request, so asking 5 nodes means 5 requests,
and if some of them are dead u wait the full timeout for each one.

A survey asks everyone at once and waits ONE timeout total.
It doesnt tell u which node answered (the reply has no sender port unless u put it in the message yourself),
and a respondent that connects after the survey was sent will miss it.

Why survey fits the bully election: `ELECTION_ASK` only needs to know "is anyone higher than me alive?".
We dont care who answered, one `OK` is enough, and no answer before the deadline means everyone higher is dead (thats basically a vote).
The REQ retry doesnt help here either, a dead node should just count as "no answer" instead of being resent to.

Sources: [nanomsg req/rep guide](https://nanomsg.org/getting-started/libnanomsg/reqrep.html),
[nanomsg survey guide](https://nanomsg.org/getting-started/libnanomsg/survey.html),
and the mangos v3 source (`options.go`, `protocol/req`, `protocol/surveyor`).

### How they connect

Upon launch, each node will ask the default port (first port of the range) for its port list.
If there is a running app the node will take the next free port and use the port list as its neighbor list.
If the default port is free, the node takes it, then checks the rest of the range in case
the old default node died but the others are still up. If nobody is up, act as a coordinator.

After launch the app will seek out all neighbors and tell them that it exists (`JOIN`).
Each app will reply them with the port of the coordinator.

#### REQ/REP version

Checking the rest of the range is done one port at a time, one REQ per port, stopping at the first node that answers.
`JOIN` is also a loop, one REQ per neighbor, and if one fails it logs which port failed (`join 9004: ...`).

#### SURVEYOR/RESPONDENT version

Checking the rest of the range is ONE survey to every port in the range, dead ports just fail to dial and get skipped.
`JOIN` is ONE survey to all neighbors. Since survey replies dont say who sent them,
a failure is logged as a count instead (`JOIN only reached 3/4 neighbors`).

### Neighbor maintaining mechanism

When a coordinator is up, each node will ping the coordinator.

For coordinator(master), each ping will have a ttl and if the servant node havent
ping in a long time, it will drop that on the neighbor list and tell
everyone that the node no longer active (`DROP`).

For the other node(servant), it will ping the coordinator and waits for a reply.
If coordinator did not reply, **this will start a election by that node**.

`PING` only goes to the coordinator so its the same in both (in the survey version its a survey with one port).

#### REQ/REP version

`DROP` is sent with one REQ per neighbor, a failed one logs the port (`drop 9004 at 9002: ...`).

#### SURVEYOR/RESPONDENT version

`DROP` is ONE survey to all neighbors, failures are logged as a count (`DROP only reached 3/4 neighbors`).

### Election

How the election work is by using election by bullying.
If a node starts an election, it will ask node with higher ports than it `ELECTION_ASK`.
If any of the node replied with `OK`, then that node will know that they
do not have the highest port and will wait.

When a node recieves `ELECTION_ASK`, they will seek out nodes higher than it again
and repeats until a node that doesnt recieve any `OK`.
That node will become coordinator and broadcasts `ELECTION_COORD` to tell everyone
that they are the current coordinator.

#### REQ/REP version

`ELECTION_ASK` is sent to each higher node one by one, every dead higher node adds a full reply timeout (1s)
before the node can decide. So with 3 dead nodes above you, deciding takes ~3s.
`ELECTION_COORD` is also one REQ per neighbor, failed ones get logged by port.

#### SURVEYOR/RESPONDENT version

`ELECTION_ASK` is ONE survey to all higher nodes, it waits at most one timeout (1 sec) no matter how many of them are dead,
and stops early if all of em already answered. One `OK` in the replies is enough to know someone higher is alive.
`ELECTION_COORD` is ONE survey to all neighbors, failures are logged as a count.

## Building it

```sh
go build -o node .
```

## Running it

Since all node uses the same binary, we can just launch the application binary serveral times.

```sh
./node &
./node &
./node &
./node &
./node &
./node &
./node &
```

with specified port range:

```sh
./node -range 10000-10100 &
./node -range 10000-10100 &
./node -range 10000-10100 &
./node -range 10000-10100 &
./node -range 10000-10100 &
./node -range 10000-10100 &
./node -range 10000-10100 &
```

Alternatively, you can just run the [run.sh](./run.sh) script.

```sh
sh ./run.sh
```

with specified port range:

```sh
bash run.sh -range 10000-10100
```

## Usefull tools

Kill app that serve port `9000`

```sh
kill $(lsof -ti:9000)
```

## Example Output

```txt
2026/10/26 13:04:47 [9000] up, neighbors []
2026/10/26 13:04:47 [9000] coordinator is 9000
2026/10/26 13:04:47 [9001] up, neighbors [9000]
2026/10/26 13:04:47 [9001] coordinator is 9000
2026/10/26 13:04:47 [9002] up, neighbors [9000 9001]
2026/10/26 13:04:47 [9002] coordinator is 9000
2026/10/26 13:04:47 [9003] up, neighbors [9000 9001 9002]
2026/10/26 13:04:47 [9003] coordinator is 9000
2026/10/26 13:04:48 [9004] up, neighbors [9000 9001 9002 9003]
2026/10/26 13:04:48 [9004] coordinator is 9000
2026/10/26 13:04:48 [9005] up, neighbors [9000 9001 9002 9003 9004]
2026/10/26 13:04:48 [9005] coordinator is 9000
2026/10/26 13:04:48 [9006] up, neighbors [9000 9001 9002 9003 9004 9005]
2026/10/26 13:04:48 [9006] coordinator is 9000
2026/10/26 13:04:50 [9000] coordinator 9000, neighbors [9001 9002 9003 9004 9005 9006]
2026/10/26 13:04:50 [9001] coordinator 9000, neighbors [9000 9002 9003 9004 9005 9006]
2026/10/26 13:04:50 [9002] coordinator 9000, neighbors [9000 9001 9003 9004 9005 9006]
2026/10/26 13:04:50 [9003] coordinator 9000, neighbors [9000 9001 9002 9004 9005 9006]
2026/10/26 13:04:51 [9004] coordinator 9000, neighbors [9000 9001 9002 9003 9005 9006]
2026/10/26 13:04:51 [9005] coordinator 9000, neighbors [9000 9001 9002 9003 9004 9006]
2026/10/26 13:04:51 [9006] coordinator 9000, neighbors [9000 9001 9002 9003 9004 9005]
2026/10/26 13:04:50 [9000] coordinator 9000, neighbors [9001 9002 9003 9004 9005 9006]
2026/10/26 13:04:50 [9001] coordinator 9000, neighbors [9000 9002 9003 9004 9005 9006]
2026/10/26 13:04:50 [9002] coordinator 9000, neighbors [9000 9001 9003 9004 9005 9006]
2026/10/26 13:04:50 [9003] coordinator 9000, neighbors [9000 9001 9002 9004 9005 9006]
2026/10/26 13:04:51 [9004] coordinator 9000, neighbors [9000 9001 9002 9003 9005 9006]
2026/10/26 13:04:51 [9005] coordinator 9000, neighbors [9000 9001 9002 9003 9004 9006]
2026/10/26 13:04:51 [9006] coordinator 9000, neighbors [9000 9001 9002 9003 9004 9005]
2026/10/26 13:05:07 [9000] no ping from 9004 for 3s, dropping it
2026/10/26 13:05:07 [9001] coordinator says 9004 is gone, dropped it
2026/10/26 13:05:07 [9002] coordinator says 9004 is gone, dropped it
2026/10/26 13:05:07 [9003] coordinator says 9004 is gone, dropped it
2026/10/26 13:05:07 [9005] coordinator says 9004 is gone, dropped it
2026/10/26 13:05:07 [9006] coordinator says 9004 is gone, dropped it
2026/10/26 13:05:08 [9000] coordinator 9000, neighbors [9001 9002 9003 9005 9006]
2026/10/26 13:05:08 [9001] coordinator 9000, neighbors [9000 9002 9003 9005 9006]
2026/10/26 13:05:08 [9002] coordinator 9000, neighbors [9000 9001 9003 9005 9006]
2026/10/26 13:05:08 [9003] coordinator 9000, neighbors [9000 9001 9002 9005 9006]
2026/10/26 13:05:09 [9005] coordinator 9000, neighbors [9000 9001 9002 9003 9006]
2026/10/26 13:05:09 [9006] coordinator 9000, neighbors [9000 9001 9002 9003 9005]
2026/10/26 13:05:41 [9000] coordinator 9000, neighbors [9001 9002 9003 9005 9006]
2026/10/26 13:05:41 [9001] coordinator 9000, neighbors [9000 9002 9003 9005 9006]
2026/10/26 13:05:41 [9002] coordinator 9000, neighbors [9000 9001 9003 9005 9006]
2026/10/26 13:05:41 [9003] coordinator 9000, neighbors [9000 9001 9002 9005 9006]
2026/10/26 13:05:42 [9005] coordinator 9000, neighbors [9000 9001 9002 9003 9006]
2026/10/26 13:05:42 [9006] coordinator 9000, neighbors [9000 9001 9002 9003 9005]
2026/10/26 13:05:44 [9005] coordinator 9000 didnt answer ping (dial tcp 127.0.0.1:9000: connect: connection refused), no coordinator now
2026/10/26 13:05:44 [9005] starting election
2026/10/26 13:05:44 [9006] got ELECTION_ASK from 9005
2026/10/26 13:05:44 [9006] starting election
2026/10/26 13:05:44 [9006] nobody higher answered, im the coordinator now
2026/10/26 13:05:44 [9006] ELECTION_COORD to 9000: dial tcp 127.0.0.1:9000: connect: connection refused
2026/10/26 13:05:44 [9001] 9006 won the election, its the coordinator now
2026/10/26 13:05:44 [9002] 9006 won the election, its the coordinator now
2026/10/26 13:05:44 [9003] 9006 won the election, its the coordinator now
2026/10/26 13:05:44 [9005] 9006 won the election, its the coordinator now
2026/10/26 13:05:44 [9001] coordinator 9006, neighbors [9000 9002 9003 9005 9006]
2026/10/26 13:05:44 [9002] coordinator 9006, neighbors [9000 9001 9003 9005 9006]
2026/10/26 13:05:44 [9003] coordinator 9006, neighbors [9000 9001 9002 9005 9006]
2026/10/26 13:05:45 [9005] coordinator 9006, neighbors [9001 9002 9003 9006]
2026/10/26 13:05:45 [9006] coordinator 9006, neighbors [9000 9001 9002 9003 9005]
2026/10/26 13:05:47 [9006] no ping from 9000 for 3s, dropping it
2026/10/26 13:05:47 [9001] coordinator says 9000 is gone, dropped it
2026/10/26 13:05:47 [9002] coordinator says 9000 is gone, dropped it
2026/10/26 13:05:47 [9003] coordinator says 9000 is gone, dropped it
2026/10/26 13:05:47 [9005] coordinator says 9000 is gone, dropped it
2026/10/26 13:05:47 [9001] coordinator 9006, neighbors [9002 9003 9005 9006]
2026/10/26 13:05:47 [9002] coordinator 9006, neighbors [9001 9003 9005 9006]
2026/10/26 13:05:47 [9003] coordinator 9006, neighbors [9001 9002 9005 9006]
2026/10/26 13:05:48 [9005] coordinator 9006, neighbors [9001 9002 9003 9006]
2026/10/26 13:05:48 [9006] coordinator 9006, neighbors [9001 9002 9003 9005]
2026/10/26 13:05:50 [9001] coordinator 9006, neighbors [9002 9003 9005 9006]
2026/10/26 13:05:50 [9002] coordinator 9006, neighbors [9001 9003 9005 9006]
2026/10/26 13:05:50 [9003] coordinator 9006, neighbors [9001 9002 9005 9006]
2026/10/26 13:05:51 [9005] coordinator 9006, neighbors [9001 9002 9003 9006]
2026/10/26 13:05:51 [9006] coordinator 9006, neighbors [9001 9002 9003 9005]
```
