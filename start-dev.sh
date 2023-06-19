#!/bin/bash
trap exit_handler SIGINT SIGTERM

function exit_handler()
{
	kill -sTERM "$pidp"
	kill -sTERM "$pidd"
}

docker run -p 5435:5432 -e POSTGRES_PASSWORD=postgres postgres &
pidd=$!

cargo watch --ignore sqlx-data.json -q -x 'sqlx prepare' > /dev/null 2>&1 &
pidp=$!

wait "$pidp"
wait "$pidd"
