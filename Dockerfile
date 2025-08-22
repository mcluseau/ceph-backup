from rust:1.89.0-alpine as build

run apk add musl-dev

copy . /src
workdir /src

run mkdir /dist
run --mount=type=cache,id=rust-alpine-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=rust-alpine-target,sharing=private,target=/src/target \
    cargo build -r \
 && mv target/release/ceph-backup /dist/

from alpine:3.22.0

entrypoint ["/bin/ceph-backup"]
run apk add --no-cache coreutils ceph-base

copy --from=build /dist/ /bin/
