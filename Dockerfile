from mcluseau/rust:1.98.1-onbuild as build

# need ceph with aes256k (min 19.2.6, 20.2.4, or 21)
#from alpine:3.24.1
from debian:forky-slim

entrypoint ["/bin/ceph-backup"]
#run apk add --no-cache coreutils ceph-base
run apt update && apt install -y ceph-base && apt clean

copy --from=build /dist/bin/ceph-backup /bin/
