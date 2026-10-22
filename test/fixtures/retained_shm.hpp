#ifndef SELF_IPCS_TEST_RETAINED_SHM_HPP
#define SELF_IPCS_TEST_RETAINED_SHM_HPP

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "edge_runtime/transport/shm_object.hpp"

namespace edge_test {

inline bool retain_shm_object(const std::string& name) {
	auto descriptor = edge_runtime::detail::shm_open_existing(name);
	if (!descriptor) return descriptor.error().errno_value == ENOENT;
	uint64_t device = 0, inode = 0, size = 0;
	if (!edge_runtime::detail::shm_fstat_and_capture(descriptor.value(), &device, &inode, &size) ||
	    name.empty() || name[0] != '/' || name.find('/', 1) != std::string::npos) {
		return false;
	}
	const char* configured = std::getenv("SELFIPCS_TEST_EVIDENCE_DIR");
	const std::filesystem::path root = configured ? configured : "metadata/retained-shm";
	std::error_code error;
	std::filesystem::create_directories(root, error);
	if (error) return false;
	const std::string filename = name.substr(1) + "." + std::to_string(::getpid());
	const auto snapshot = root / (filename + ".bin");
	std::FILE* output = std::fopen(snapshot.c_str(), "wbx");
	if (output == nullptr) return false;
	bool copied = size <= 64 * 1024 * 1024;
	uint64_t offset = 0;
	while (copied && offset < size) {
		char bytes[4096];
		const size_t wanted = static_cast<size_t>(std::min<uint64_t>(sizeof(bytes), size - offset));
		const ssize_t count = ::pread(descriptor.value().get(), bytes, wanted,
		                              static_cast<off_t>(offset));
		if (count < 0 && errno == EINTR) continue;
		if (count <= 0 || std::fwrite(bytes, 1, static_cast<size_t>(count), output) !=
		                          static_cast<size_t>(count)) {
			copied = false;
			break;
		}
		offset += static_cast<uint64_t>(count);
	}
	const bool closed = std::fclose(output) == 0;
	const auto identity = root / (filename + ".txt");
	output = std::fopen(identity.c_str(), "wx");
	if (output == nullptr) return false;
	const int written = std::fprintf(output, "name=%s\ndev=%llu\nino=%llu\nsize=%llu\nsnapshot=%s\n",
	                                 name.c_str(), static_cast<unsigned long long>(device),
	                                 static_cast<unsigned long long>(inode),
	                                 static_cast<unsigned long long>(size), snapshot.c_str());
	const bool identity_closed = std::fclose(output) == 0;
	std::printf("RETAINED_SHM name=%s inode=%llu size=%llu snapshot=%s\n", name.c_str(),
	            static_cast<unsigned long long>(inode), static_cast<unsigned long long>(size),
	            snapshot.c_str());
	return copied && closed && written >= 0 && identity_closed;
}

inline bool preserve_aba_creator(const std::string& name) {
	const std::string retained = name + ".creator-retained";
	return ::link(("/dev/shm" + name).c_str(), ("/dev/shm" + retained).c_str()) == 0 &&
	       retain_shm_object(retained);
}

}

#endif
