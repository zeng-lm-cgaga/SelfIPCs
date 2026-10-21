#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "notification_probe.h"
#include "self_ipcs_notification.h"

_Static_assert(sizeof(__u64) == SELF_IPCS_NOTIFICATION_COUNTER_BYTES, "counter ABI width");
_Static_assert(_IOC_SIZE(SELF_IPCS_NOTIFICATION_STOP) == 0, "stop has no payload");

int main(int argc, char** argv) {
	if (argc == 2 && strcmp(argv[1], "--abi-check") == 0) {
		printf("PASS compile-time UAPI: counter=%u bytes; STOP=0x%lx; no device opened\n",
		       SELF_IPCS_NOTIFICATION_COUNTER_BYTES,
		       (unsigned long)SELF_IPCS_NOTIFICATION_STOP);
		return 0;
	}
	if (argc == 3 && strcmp(argv[1], "--peer") == 0) {
		char* end = NULL;
		long descriptor;

		errno = 0;
		descriptor = strtol(argv[2], &end, 10);
		if (errno || end == argv[2] || *end || descriptor < 0 || descriptor > INT_MAX)
			return 2;
		return notification_probe_peer((int)descriptor);
	}
	if (argc == 1) return notification_probe_run("/dev/" SELF_IPCS_NOTIFICATION_DEVICE);
	if (argc == 3 && strcmp(argv[1], "--device") == 0) return notification_probe_run(argv[2]);
	fprintf(stderr, "usage: %s [--abi-check | --device PATH]\n", argv[0]);
	return 2;
}
