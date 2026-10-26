#!/bin/sh
# starts 7 nodes, each one is its own process
# we can use Ctrl+C to stop them all
cd "$(dirname "$0")"
go build -o node . || exit 1

trap 'kill $(jobs -p) 2>/dev/null' INT TERM EXIT
for i in 1 2 3 4 5 6 7; do
	./node "$@" &
done
wait
