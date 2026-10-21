#ifndef SELFIPCS_MIXED_IO_DEMO_API_HPP
#define SELFIPCS_MIXED_IO_DEMO_API_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#if defined(SELFIPCS_DEMO_USE_LEGACY)
#include "edge_runtime/consumer.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/producer.hpp"
#include "edge_runtime/schema.hpp"
#else
#include "self_ipcs/channel/consumer.hpp"
#include "self_ipcs/channel/producer.hpp"
#include "self_ipcs/common/error.hpp"
#include "self_ipcs/common/schema.hpp"
#endif

namespace mixed_io_demo {

#if defined(SELFIPCS_DEMO_USE_LEGACY)
namespace ipc = edge_runtime;
#else
namespace ipc = self_ipcs;
#endif

struct Message {
	uint64_t sequence{0};
	uint64_t final{0};
};

struct Options {
	std::string interface;
	std::string channel;
	uint32_t duration_ms{300};
	uint32_t can_batch{8};
	uint32_t can_id{0};
	int control_fd{-1};
	int stop_fd{-1};
};

inline ipc::SchemaDescriptor schema() {
	ipc::SchemaDescriptor result;
	result.fingerprint = {std::byte{0x6d}, std::byte{0x69}, std::byte{0x78}, std::byte{0x69},
	                      std::byte{0x6f}, std::byte{0x01}};
	result.version = 1;
	result.debug_name = "mixed_io_demo.u64le.v1";
	return result;
}

bool run_producer(const Options& options);
bool run_consumer(const Options& options);

}

template <>
struct edge_runtime::PayloadCodec<mixed_io_demo::Message> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 16;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const mixed_io_demo::Message& value, std::byte* output,
	                   size_t capacity) noexcept {
		if (output == nullptr || capacity < kEncodedSize) return false;
		for (uint32_t index = 0; index < 8; ++index) {
			output[index] = static_cast<std::byte>((value.sequence >> (index * 8U)) & 0xffU);
			output[index + 8] = static_cast<std::byte>((value.final >> (index * 8U)) & 0xffU);
		}
		return true;
	}

	static bool decode(const std::byte* input, size_t size,
	                   mixed_io_demo::Message* value) noexcept {
		if (input == nullptr || value == nullptr || size != kEncodedSize) return false;
		*value = {};
		for (uint32_t index = 0; index < 8; ++index) {
			value->sequence |= std::to_integer<uint64_t>(input[index]) << (index * 8U);
			value->final |= std::to_integer<uint64_t>(input[index + 8]) << (index * 8U);
		}
		return true;
	}
};

#endif
