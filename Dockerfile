from rust:1.83.0-alpine as build

run apk add musl-dev

copy . /src
workdir /src

run cargo build -r

from alpine:3.21.0

entrypoint ["/bin/ceph-backup"]
run apk add --no-cache coreutils ceph-base

copy --from=build /src/target/release/ceph-backup /bin/
