#!/usr/bin/env bash
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts a single-node Redis Software cluster with flash storage in Docker
# (the redis-flex service in compose.yaml), creates a Redis Flex database
# with Search on it, and prints the database's URI. Running it again reuses
# the cluster and the database.
#
# For testing only: the "flash" is a directory inside the container, the
# cluster runs on the built-in trial license, and the admin credentials are
# the fixed defaults below. Needs Docker, curl and jq.
#
#   ./flex-setup.sh                                # prints REDIS_URI=redis://localhost:12000/0
#   docker compose --profile flex down redis-flex  # removes the cluster
set -euo pipefail
cd "$(dirname "$0")"

api_port=${FLEX_API_PORT:-9443}
db_port=${FLEX_DB_PORT:-12000}
admin_user=${FLEX_ADMIN_USER:-admin@example.com}
admin_pass=${FLEX_ADMIN_PASSWORD:-adbc-flex-test}
redis_version=${FLEX_REDIS_VERSION:-8.6}
# The database's size and the part of it kept in RAM; the rest is on flash.
memory_mb=${FLEX_MEMORY_MB:-1024}
ram_mb=${FLEX_RAM_MB:-128}
db_name=adbc-flex

api="https://localhost:$api_port/v1"
# The cluster's certificate is self-signed.
call() { curl -sSk -u "$admin_user:$admin_pass" -H 'Content-Type: application/json' "$@"; }
last_reply=
wait_for() { # what, command...
	local what=$1
	shift
	for _ in $(seq 120); do
		if "$@"; then return 0; fi
		sleep 2
	done
	echo "timed out waiting for $what${last_reply:+; last reply: $last_reply}" >&2
	exit 1
}

FLEX_API_PORT=$api_port FLEX_DB_PORT=$db_port docker compose --profile flex up --detach --wait redis-flex >&2

# Before the cluster exists the API rejects credentials; afterwards it may
# require them.
state() {
	{ curl -sSk "$api/bootstrap" | jq -er .bootstrap_status.state 2>/dev/null; } ||
		call "$api/bootstrap" | jq -r .bootstrap_status.state
}
if [ "$(state)" = idle ]; then
	echo "creating the cluster (flash in /var/opt/redislabs/flash)" >&2
	jq -n --arg user "$admin_user" --arg pass "$admin_pass" '{
		action: "create_cluster",
		cluster: {name: "cluster.local"},
		node: {
			bigstore_enabled: true,
			paths: {
				persistent_path: "/var/opt/redislabs/persist",
				ephemeral_path: "/var/opt/redislabs/tmp",
				bigstore_path: "/var/opt/redislabs/flash"
			}
		},
		credentials: {username: $user, password: $pass}
	}' | curl -sSkf -H 'Content-Type: application/json' -X POST "$api/bootstrap/create_cluster" -d @- >/dev/null
fi
bootstrapped() {
	local s
	s=$(state)
	if [ "$s" = failed ]; then
		call "$api/bootstrap" | jq .bootstrap_status >&2
		exit 1
	fi
	[ "$s" = completed ] && call -f -o /dev/null "$api/cluster"
}
wait_for "the cluster" bootstrapped

uid=$(call -f "$api/bdbs" | jq --arg name "$db_name" '[.[] | select(.name == $name) | .uid][0] // empty')
if [ -z "$uid" ]; then
	echo "creating Flex database $db_name: ${memory_mb} MB, ${ram_mb} MB of it in RAM, Redis $redis_version" >&2
	# Redis 8 databases get Search, JSON and Bloom without a module_list
	# (passing one fails with an internal error on 8.2.0-78). Search on
	# Flex needs Redis 8.6 or later.
	# The port is the container's; compose maps FLEX_DB_PORT to it.
	body=$(jq -n --arg name "$db_name" --arg version "$redis_version" \
		--argjson memory "$((memory_mb * 1024 * 1024))" --argjson ram "$((ram_mb * 1024 * 1024))" '{
		name: $name,
		type: "redis",
		redis_version: $version,
		port: 12000,
		memory_size: $memory,
		bigstore: true,
		bigstore_ram_size: $ram,
		search_on_bigstore: true
	}')
	# A just-created cluster refuses new databases (406) for a few seconds.
	create() {
		last_reply=$(call -X POST "$api/bdbs" -d "$body") || return 1
		uid=$(jq -er .uid <<<"$last_reply" 2>/dev/null)
	}
	wait_for "database $db_name to be accepted" create
	last_reply=
fi
active() { [ "$(call -f "$api/bdbs/$uid" | jq -r .status)" = active ]; }
wait_for "database $db_name" active

call -f "$api/bdbs/$uid" | jq -r '"\(.name): Redis \(.version), bigstore \(.bigstore) (version \(.bigstore_version)), " +
	"\(.memory_size / 1048576) MB with \(.bigstore_ram_size / 1048576) MB in RAM, " +
	"modules: \([.module_list[].module_name] | join(", "))"' >&2
echo "REDIS_URI=redis://localhost:$db_port/0"
