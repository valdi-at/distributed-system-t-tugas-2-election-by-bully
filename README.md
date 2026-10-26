# Tugas 2 - Election by Bullying

| Name                        | NRP        |
| --------------------------- | ---------- |
| Mohamad Valdi Ananda Tauhid | 5025221238 |

## General Structure

Each node is its own application using the same binary.
Each node listens on a port automatically generated when launching
(within the port address bounds).

### How they connect

Upon launch, each node will check the given port range for an existing running app.
If there is a running app the node will ask the app for their neighbor list.
If there is no app, act as a coordinator and use the lower-most port.

After launch the app will seek out all neighbors and tell them that it exists.
Each app will reply them with the port of the coordinator.

### Neighbor maintaining mechanism

When a coordinator is up, each node will ping the coordinator.

For coordinator(master), each ping will have a ttl and if the servant node havent
ping in a long time, it will drop that on the neighbor list and tell
everyone that the node no longer active.

For the other node(servant), it will ping the coordinator and waits for a reply.
If coordinator did not reply, **this will start a election by that node**.

### Election

How the election work is by using election by bullying.
If a node starts an election, it will ask node with higher ports than it `ELECTION_ASK`.
If any of the node replied with `OK`, then that node will know that they
do not have the highest port and will wait.

When a node recieves `ELECTION_ASK`, they will seek out nodes higher than it again
and repeats until a node that doesnt recieve any `OK`.
That node will become coordinator and broadcasts `ELECTION_COORD` to tell everyone
that they are the current coordinator.

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
