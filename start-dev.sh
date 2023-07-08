#!/bin/bash
trap exit_handler SIGINT SIGTERM

function exit_handler()
{
	rm .env
	kill -sTERM "$pidp"
	kill -sTERM "$pidd"
}

docker run -p 5435:5432 -e POSTGRES_PASSWORD=postgres postgres &
pidd=$!

echo "DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5435/postgres" > .env

cargo watch -w src/meta/postgres -q -x 'sqlx prepare' > /dev/null 2>&1 &
pidp=$!

wait "$pidp"
wait "$pidd"
