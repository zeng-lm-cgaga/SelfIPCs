#include "self_ipcs_notification.h"

#include <linux/compat.h>
#include <linux/fs.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/poll.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/uaccess.h>
#include <linux/wait.h>

struct notification_state {
	struct mutex operation_lock;
	spinlock_t state_lock;
	wait_queue_head_t waiters;
	__u64 pending;
	bool stopped;
};

static int notification_lock(struct file* file, struct notification_state* state) {
	if (file->f_flags & O_NONBLOCK) return mutex_trylock(&state->operation_lock) ? 0 : -EAGAIN;
	return mutex_lock_interruptible(&state->operation_lock) ? -EINTR : 0;
}

static bool notification_ready(struct notification_state* state) {
	bool ready;

	spin_lock(&state->state_lock);
	ready = state->pending != 0 || state->stopped;
	spin_unlock(&state->state_lock);
	return ready;
}

static void notification_wake(struct notification_state* state) {
	wake_up_interruptible_poll(&state->waiters,
	                           EPOLLIN | EPOLLRDNORM | EPOLLOUT | EPOLLWRNORM | EPOLLHUP);
}

static int notification_open(struct inode* inode, struct file* file) {
	struct notification_state* state;
	int result;

	state = kzalloc(sizeof(*state), GFP_KERNEL);
	if (!state) return -ENOMEM;
	mutex_init(&state->operation_lock);
	spin_lock_init(&state->state_lock);
	init_waitqueue_head(&state->waiters);
	result = stream_open(inode, file);
	if (result) {
		kfree(state);
		return result;
	}
	file->private_data = state;
	return 0;
}

static int notification_release(struct inode* inode, struct file* file) {
	(void)inode;
	kfree(file->private_data);
	return 0;
}

static ssize_t notification_read(struct file* file, char __user* buffer, size_t length,
                                 loff_t* position) {
	struct notification_state* state = file->private_data;
	__u64 snapshot;
	bool stopped;
	int result;

	(void)position;
	if (length != sizeof(snapshot)) return -EINVAL;
	for (;;) {
		result = notification_lock(file, state);
		if (result) return result;
		spin_lock(&state->state_lock);
		snapshot = state->pending;
		stopped = state->stopped;
		spin_unlock(&state->state_lock);
		if (snapshot) {
			if (copy_to_user(buffer, &snapshot, sizeof(snapshot))) {
				mutex_unlock(&state->operation_lock);
				return -EFAULT;
			}
			spin_lock(&state->state_lock);
			state->pending -= snapshot;
			spin_unlock(&state->state_lock);
			mutex_unlock(&state->operation_lock);
			notification_wake(state);
			return sizeof(snapshot);
		}
		mutex_unlock(&state->operation_lock);
		if (stopped) return 0;
		if (file->f_flags & O_NONBLOCK) return -EAGAIN;
		result = wait_event_interruptible(state->waiters, notification_ready(state));
		if (result) return -EINTR;
	}
}

static ssize_t notification_write(struct file* file, const char __user* buffer, size_t length,
                                  loff_t* position) {
	struct notification_state* state = file->private_data;
	__u64 increment;
	int result;

	(void)position;
	if (length != sizeof(increment)) return -EINVAL;
	result = notification_lock(file, state);
	if (result) return result;
	if (copy_from_user(&increment, buffer, sizeof(increment))) {
		mutex_unlock(&state->operation_lock);
		return -EFAULT;
	}
	spin_lock(&state->state_lock);
	if (state->stopped)
		result = -EPIPE;
	else if (!increment)
		result = -EINVAL;
	else if (increment > SELF_IPCS_NOTIFICATION_MAX - state->pending)
		result = -EOVERFLOW;
	else {
		state->pending += increment;
		result = sizeof(increment);
	}
	spin_unlock(&state->state_lock);
	mutex_unlock(&state->operation_lock);
	if (result > 0) notification_wake(state);
	return result;
}

static long notification_ioctl(struct file* file, unsigned int command, unsigned long argument) {
	struct notification_state* state = file->private_data;
	int result;

	if (command != SELF_IPCS_NOTIFICATION_STOP) return -ENOTTY;
	if (argument) return -EINVAL;
	result = notification_lock(file, state);
	if (result) return result;
	spin_lock(&state->state_lock);
	state->stopped = true;
	spin_unlock(&state->state_lock);
	mutex_unlock(&state->operation_lock);
	notification_wake(state);
	return 0;
}

static __poll_t notification_poll(struct file* file, poll_table* wait) {
	struct notification_state* state = file->private_data;
	__poll_t events = 0;

	poll_wait(file, &state->waiters, wait);
	spin_lock(&state->state_lock);
	if (state->pending)
		events |= EPOLLIN | EPOLLRDNORM;
	else if (state->stopped)
		events |= EPOLLHUP;
	if (!state->stopped && state->pending < SELF_IPCS_NOTIFICATION_MAX)
		events |= EPOLLOUT | EPOLLWRNORM;
	spin_unlock(&state->state_lock);
	return events;
}

static const struct file_operations notification_operations = {
        .owner = THIS_MODULE,
        .open = notification_open,
        .release = notification_release,
        .read = notification_read,
        .write = notification_write,
        .unlocked_ioctl = notification_ioctl,
#ifdef CONFIG_COMPAT
        .compat_ioctl = notification_ioctl,
#endif
        .poll = notification_poll,
        .llseek = no_llseek,
};

static struct miscdevice notification_device = {
        .minor = MISC_DYNAMIC_MINOR,
        .name = SELF_IPCS_NOTIFICATION_DEVICE,
        .fops = &notification_operations,
        .mode = 0600,
};

module_misc_device(notification_device);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Optional per-open software notification experiment");
