#from mcluseau/rust:1.99.1-onbuild as build
from debian:forky-slim as build
run apt update && apt install -y build-essential rustup librados-dev
run rustup install 1.100.0

copy . /src
workdir /src

run \
  --mount=type=ssh \
  --mount=type=cache,id=rust-forky-registry,target=/usr/local/cargo/registry \
  --mount=type=cache,id=rust-forky-target,sharing=private,target=/src/target \
  cargo install --path . --root /dist

# need ceph with aes256k (min 19.2.6, 20.2.4, or 21)
#from alpine:3.24.2
from debian:forky-slim

entrypoint ["/bin/ceph-backup"]
#run apk add --no-cache coreutils ceph19-base
run apt update && apt install -y ceph-base && apt clean

copy --from=build /dist/bin/ceph-backup /bin/
