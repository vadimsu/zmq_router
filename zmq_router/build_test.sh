rm -f bm
gcc -Ofast test.c ../build/libnetinet.a ../dpdk_libs/libdpdk.a build/lib/libperfm -lpthread -lrt -ldl -o bm
