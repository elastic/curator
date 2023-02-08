ARCH=$(uname -m)

ln -s /lib/libc.musl-${ARCH}.so.1 ldd
ln -s /lib /lib64
