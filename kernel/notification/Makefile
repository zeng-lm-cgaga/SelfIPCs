KERNEL_BUILD ?= /lib/modules/$(shell uname -r)/build
OUTPUT_ROOT ?= /tmp

.PHONY: all build probe
all: build

build:
	python3 ../../scripts/build_software_notification.py --kernel-build "$(KERNEL_BUILD)" --output-root "$(OUTPUT_ROOT)"

probe:
	python3 ../../scripts/build_software_notification.py --userspace-only --output-root "$(OUTPUT_ROOT)"
