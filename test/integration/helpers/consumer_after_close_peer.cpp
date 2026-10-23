#include <chrono>
#include <cstring>
#include <iostream>

#ifdef USE_LEGACY
#include "edge_runtime/channel/consumer.hpp"
namespace sdk = edge_runtime;
#else
#include "self_ipcs/channel/consumer.hpp"
namespace sdk = self_ipcs;
#endif
#include "test_payload.hpp"

int main(int argc, char** argv) {
	using namespace std::chrono_literals;
	if (argc != 5) return 2;
	sdk::ChannelOptions options;
	options.name = argv[1];
	if (std::strcmp(argv[2], "memfd") == 0) options.transport = sdk::Transport::kMemfdFdPass;
	auto context = sdk::ControlOperationContext::after(2s);
	if (!context) return 3;
	sdk::ControlReceipt receipt;
	auto consumer = sdk::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema(),
	                                                   context.value(), receipt);
	if (!consumer) return 4;
	auto closed = std::strcmp(argv[3], "finalize") == 0
	                      ? consumer.value().finalize(context.value(), receipt)
	                      : consumer.value().close(context.value(), receipt);
	if (!closed || closed.value() != sdk::ControlCloseStatus::kClosed) return 5;
	std::cout << "child opened independently; " << argv[3]
	          << "=Closed; notification=" << argv[4] << std::endl;
	auto notification = [&]() -> sdk::Result<sdk::EventNotification> {
		if (std::strcmp(argv[4], "default") == 0)
			return consumer.value().event_notification(20ms);
		if (std::strcmp(argv[4], "backend") == 0)
			return consumer.value().event_notification(
			        sdk::NotificationBackend::kEventFd, 20ms);
		return consumer.value().event_notification(context.value(), receipt);
	}();
	if (notification || notification.error().code != sdk::ErrorCode::kClosed) return 6;
	auto rejected = [](const auto& result) {
		return !result && result.error().code == sdk::ErrorCode::kClosed;
	};
	if (!rejected(consumer.value().try_read_latest()) ||
	    !rejected(consumer.value().wait_latest(1ms)) ||
	    !rejected(consumer.value().try_loan_latest()) ||
	    !rejected(consumer.value().wait_loan_latest(1ms)) ||
	    !rejected(consumer.value().status()) || !rejected(consumer.value().reconnect()) ||
	    !rejected(consumer.value().reconnect(context.value(), receipt)))
		return 7;
	consumer.value().request_stop();
	closed = consumer.value().close(context.value(), receipt);
	return closed && closed.value() == sdk::ControlCloseStatus::kClosed ? 0 : 8;
}
