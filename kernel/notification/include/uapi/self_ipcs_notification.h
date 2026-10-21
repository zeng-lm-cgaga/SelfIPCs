#ifndef SELF_IPCS_NOTIFICATION_H
#define SELF_IPCS_NOTIFICATION_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define SELF_IPCS_NOTIFICATION_DEVICE "self_ipcs_notification"
#define SELF_IPCS_NOTIFICATION_COUNTER_BYTES 8U
#define SELF_IPCS_NOTIFICATION_MAX ((__u64)~0ULL)
#define SELF_IPCS_NOTIFICATION_STOP _IO(0xB7, 0x01)

#endif
