#!/bin/sh
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the Linux driver library in an AlmaLinux 8 container. Linking
# against RHEL 8's glibc (2.28), the oldest one among the platforms Redis
# Software supports, gives one library that loads on all of them: RHEL 8
# and 9, Ubuntu 20.04 and later, and Amazon Linux 2023.
#
#   ./build-linux.sh amd64|arm64 [version]
#
# Writes build/linux-<arch>/libadbc_driver_redis.so and its header. Needs
# Docker; a foreign architecture runs under emulation and is much slower.
set -eu
cd "$(dirname "$0")"

arch=$1
version=${2:-v0.1.0}
# The Go release go.mod asks for ("1.27" is released as "1.27.0").
go_version=$(awk '$1 == "go" { print $2 }' go.mod)
case $go_version in *.*.*) ;; *) go_version=$go_version.0 ;; esac

docker run --rm --platform "linux/$arch" -v "$PWD:/src" -w /src \
  -e ARCH="$arch" -e VERSION="$version" -e GO_VERSION="$go_version" -e OWNER="$(id -u):$(id -g)" \
  almalinux:8 sh -euc '
    dnf -q -y install gcc tar gzip >/dev/null
    curl -sSfL "https://go.dev/dl/go$GO_VERSION.linux-$ARCH.tar.gz" | tar -C /usr/local -xz
    out=build/linux-$ARCH
    CGO_ENABLED=1 /usr/local/go/bin/go build -buildvcs=false -buildmode=c-shared -tags driverlib \
      -ldflags "-s -w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=$VERSION" \
      -o "$out/libadbc_driver_redis.so" ./pkg
    chown -R "$OWNER" build
    glibc=$(objdump -T "$out/libadbc_driver_redis.so" | grep -o "GLIBC_[0-9.]*" | sort -uV | tail -1)
    echo "$out/libadbc_driver_redis.so requires $glibc"
    [ "$(printf "%s\n" "$glibc" GLIBC_2.28 | sort -V | tail -1)" = GLIBC_2.28 ]'
