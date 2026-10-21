#ifndef SELF_IPCS_NOTIFICATION_DEMO_API_HPP
#define SELF_IPCS_NOTIFICATION_DEMO_API_HPP

#include <array>
#include <cstddef>
#include <cstdint>

#if defined(SELFIPCS_DEMO_USE_LEGACY)
#include "edge_runtime/consumer.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/producer.hpp"
#include "edge_runtime/schema.hpp"
namespace ipc = edge_runtime;
#else
#include "self_ipcs/channel/consumer.hpp"
#include "self_ipcs/channel/producer.hpp"
#include "self_ipcs/common/error.hpp"
#include "self_ipcs/common/schema.hpp"
namespace ipc = self_ipcs;
#endif

namespace notification_demo {

struct Message {
	uint32_t value{0};
};

inline ipc::SchemaDescriptor message_schema() {
	ipc::SchemaDescriptor schema;
	schema.fingerprint = {std::byte{0x6e}, std::byte{0x6f}, std::byte{0x74}, std::byte{0x69},
	                      std::byte{0x66}, std::byte{0x79}, std::byte{0x01}};
	schema.version = 1;
	schema.debug_name = "notification_demo.u32le.v1";
	return schema;
}

}

template <>
#if defined(SELFIPCS_DEMO_USE_LEGACY)
struct edge_runtime::PayloadCodec<notification_demo::Message> {
#else
struct self_ipcs::PayloadCodec<notification_demo::Message> {
#endif
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 4;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const notification_demo::Message& value, std::byte* output,
	                   size_t capacity) noexcept {
		if (output == nullptr || capacity < kEncodedSize) return false;
		for (uint32_t index = 0; index < kEncodedSize; ++index) {
			output[index] = static_cast<std::byte>((value.value >> (index * 8U)) & 0xffU);
		}
		return true;
	}

	static bool decode(const std::byte* input, size_t size,
	                   notification_demo::Message* value) noexcept {
		if (input == nullptr || value == nullptr || size != kEncodedSize) return false;
		value->value = 0;
		for (uint32_t index = 0; index < kEncodedSize; ++index) {
			value->value |= std::to_integer<uint32_t>(input[index]) << (index * 8U);
		}
		return true;
	}
};

#endif
