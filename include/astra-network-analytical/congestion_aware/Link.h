/****************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#pragma once

#include "common/EventQueue.h"
#include "common/Type.h"
#include "congestion_aware/Type.h"
#include <cstdint>
#include <fstream>
#include <list>
#include <limits>
#include <memory>
#include <string>
#include <vector>

using namespace NetworkAnalytical;

namespace NetworkAnalyticalCongestionAware {

class Link {
  public:
    static void configure_quantization(bool enabled,
                                       double ratio,
                                       uint64_t queue_threshold,
                                       const std::string& policy,
                                       const std::string& metric,
                                       double threshold,
                                       const std::string& metadata_mode,
                                       uint64_t metadata_bytes_per_chunk,
                                       uint64_t metadata_bytes_per_group,
                                       uint64_t metadata_group_size_bytes) noexcept;

    static void report_quantization_stats() noexcept;
    static void link_become_free(void* link_ptr) noexcept;
    static void set_event_queue(std::shared_ptr<EventQueue> event_queue_ptr) noexcept;

    Link(Bandwidth bandwidth, Latency latency) noexcept;
    void send(std::unique_ptr<Chunk> chunk) noexcept;
    void process_pending_transmission() noexcept;
    [[nodiscard]] bool pending_chunk_exists() const noexcept;
    void set_busy() noexcept;
    void set_free() noexcept;

  private:
    static std::shared_ptr<EventQueue> event_queue;
    static bool quantization_enabled;
    static double quantization_ratio;
    static uint64_t quantization_queue_threshold;
    static std::string quantization_policy;
    static std::string quantization_metric;
    static double quantization_threshold;
    static std::string quantization_metadata_mode;
    static uint64_t quantization_metadata_bytes_per_chunk;
    static uint64_t quantization_metadata_bytes_per_group;
    static uint64_t quantization_metadata_group_size_bytes;
    static std::vector<Link*> all_links;
    static bool quantization_reported;
    static std::ofstream quantization_event_stream;
    static bool quantization_event_header_written;

    Bandwidth bandwidth;
    Bandwidth bandwidth_Bpns;
    Latency latency;
    std::list<std::unique_ptr<Chunk>> pending_chunks;
    bool busy;
    uint64_t queue_depth_at_last_update;
    EventTime queue_depth_last_update_time;
    EventTime time_above_threshold;
    uint64_t queue_depth_samples;
    uint64_t queue_depth_samples_above_threshold;
    uint64_t max_queue_depth_observed;
    uint64_t transmitted_chunks;
    uint64_t quantized_chunks;
    uint64_t original_transmitted_bytes;
    uint64_t effective_transmitted_bytes;
    uint64_t quantization_metadata_transmitted_bytes;
    uint64_t in_flight_effective_bytes;
    EventTime busy_time;
    EventTime busy_state_change_time;

    [[nodiscard]] EventTime serialization_delay(ChunkSize chunk_size) const noexcept;
    [[nodiscard]] EventTime communication_delay(ChunkSize chunk_size) const noexcept;
    [[nodiscard]] uint64_t current_congestion_level() const noexcept;
    void update_queue_time_accounting(EventTime now) noexcept;
    [[nodiscard]] double current_congestion_score(EventTime now,
                                                  ChunkSize prospective_bytes = 0) const noexcept;
    [[nodiscard]] double selected_quantization_ratio(double congestion_score) const noexcept;
    [[nodiscard]] ChunkSize effective_chunk_size(ChunkSize original_size,
                                                 double selected_ratio,
                                                 uint64_t& metadata_bytes) const noexcept;
    static void ensure_quantization_event_stream() noexcept;
    void log_quantization_event(const Chunk& chunk,
                                ChunkSize original_size,
                                ChunkSize modeled_size,
                                ChunkSize payload_size_after_quantization,
                                uint64_t metadata_bytes,
                                bool quantized,
                                uint64_t congestion_level,
                                double congestion_score,
                                double applied_ratio,
                                EventTime current_time) noexcept;
    void schedule_chunk_transmission(std::unique_ptr<Chunk> chunk) noexcept;
};

}  // namespace NetworkAnalyticalCongestionAware
