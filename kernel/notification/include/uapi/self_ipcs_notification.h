#ifndef SELF_IPCS_NOTIFICATION_H
#define SELF_IPCS_NOTIFICATION_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define SELF_IPCS_NOTIFICATION_DEVICE "self_ipcs_notification"
#define SELF_IPCS_NOTIFICATION_COUNTER_BYTES 8U
#define SELF_IPCS_NOTIFICATION_MAX ((__u64)~0ULL)
#define SELF_IPCS_NOTIFICATION_STOP _IO(0xB7, 0x01)
#define SELF_IPCS_NOTIFICATION_ABI 1U
#define SELF_IPCS_NOTIFICATION_CAP_COUNTER (1ULL << 0)
#define SELF_IPCS_NOTIFICATION_CAP_STOP (1ULL << 1)
#define SELF_IPCS_NOTIFICATION_CAP_INDEPENDENT_WRITES (1ULL << 2)

struct self_ipcs_notification_info {
	__u32 abi_version;
	__u32 struct_size;
	__u64 capabilities;
	__u64 reserved[2];
};

#define SELF_IPCS_NOTIFICATION_GET_INFO _IOR(0xB7, 0x02, struct self_ipcs_notification_info)

#endif
