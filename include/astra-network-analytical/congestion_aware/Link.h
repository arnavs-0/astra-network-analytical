/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#pragma once

#include "common/EventQueue.h"
#include "common/Type.h"
#include "congestion_aware/Type.h"
#include <cstdint>
#include <fstream>
#include <limits>
#include <memory>
#include <vector>

using namespace NetworkAnalytical;

namespace NetworkAnalyticalCongestionAware {

/**
 * Link models physical links between two devices.
 */
class Link {
  public:
    /**
     * Configure quantization behavior for all links.
     *
     * @param enabled true to enable quantization-aware chunk shrinking
     * @param ratio effective size ratio applied to quantized chunks
     * @param queue_threshold queue depth threshold to trigger quantization
     */
    static void configure_quantization(bool enabled,
                                       double ratio,
                                       uint64_t queue_threshold) noexcept;

    /**
     * Print aggregated quantization and congestion statistics for all links.
     * Intended for end-of-simulation reporting.
     */
    static void report_quantization_stats() noexcept;

    /**
     * Callback to be called when a link becomes free.
     *  - If the link has pending chunks, process the first one.
     *  - If the link has no pending chunks, set the link as free.
     *
     * @param link_ptr pointer to the link that becomes free
     */
    static void link_become_free(void* link_ptr) noexcept;

    /**
     * Set the event queue to be used by the link.
     *
     * @param event_queue_ptr pointer to the event queue
     */
    static void set_event_queue(std::shared_ptr<EventQueue> event_queue_ptr) noexcept;

    /**
     * Constructor.
     *
     * @param bandwidth bandwidth of the link
     * @param latency latency of the link
     */
    Link(Bandwidth bandwidth, Latency latency) noexcept;

    /**
     * Try to send a chunk through the link.
     * - If the link is free, service the chunk immediately.
     * - If the link is busy, add the chunk to the pending chunks list.
     *
     * @param chunk the chunk to be served by the link
     */
    void send(std::unique_ptr<Chunk> chunk) noexcept;

    /**
     * Dequeue and try to send the first pending chunk
     * in the pending chunks list.
     */
    void process_pending_transmission() noexcept;

    /**
     * Check if the link has pending chunks.
     *
     * @return true if the link has pending chunks, false otherwise
     */
    [[nodiscard]] bool pending_chunk_exists() const noexcept;

    /**
     * Set the link as busy.
     */
    void set_busy() noexcept;

    /**
     * Set the link as free.
     */
    void set_free() noexcept;

  private:
    /// event queue Link uses to schedule events
    static std::shared_ptr<EventQueue> event_queue;

    /// global policy switch for quantization-aware sizing
    static bool quantization_enabled;

    /// quantized size ratio in (0, 1]
    static double quantization_ratio;

    /// queue depth threshold that enables quantization when reached
    static uint64_t quantization_queue_threshold;

    /// all link instances to support aggregated statistics reporting
    static std::vector<Link*> all_links;

    /// guard to avoid duplicate reporting
    static bool quantization_reported;

    /// output stream for per-transmission quantization event records
    static std::ofstream quantization_event_stream;

    /// guard for writing quantization event CSV header
    static bool quantization_event_header_written;

    /// bandwidth of the link in GB/s
    Bandwidth bandwidth;

    /// bandwidth of the link in B/ns, used in actual computation
    Bandwidth bandwidth_Bpns;

    /// latency of the link in ns
    Latency latency;

    /// queue of pending chunks
    std::list<std::unique_ptr<Chunk>> pending_chunks;

    /// flag to indicate if the link is busy
    bool busy;

    /// queue depth tracked since last update
    uint64_t queue_depth_at_last_update;

    /// timestamp when queue depth was last updated
    EventTime queue_depth_last_update_time;

    /// cumulative time spent above the quantization threshold
    EventTime time_above_threshold;

    /// queue depth observations made at scheduling points
    uint64_t queue_depth_samples;

    /// queue depth observations above threshold
    uint64_t queue_depth_samples_above_threshold;

    /// max observed pending queue depth
    uint64_t max_queue_depth_observed;

    /// transmitted chunk count on this link
    uint64_t transmitted_chunks;

    /// quantized chunk count on this link
    uint64_t quantized_chunks;

    /// bytes requested by upper layers before quantization
    uint64_t original_transmitted_bytes;

    /// effective bytes used by delay model after quantization
    uint64_t effective_transmitted_bytes;

    /**
     * Compute the serialization delay of a chunk on the link.
     * i.e., serialization delay = (chunk size) / (link bandwidth)
     *
     * @param chunk_size size of the target chunk
     * @return serialization delay of the chunk
     */
    [[nodiscard]] EventTime serialization_delay(ChunkSize chunk_size) const noexcept;

    /**
     * Compute the communication delay of a chunk.
     * i.e., communication delay = (link latency) + (serialization delay)
     *
     * @param chunk_size size of the target chunk
     * @return communication delay of the chunk
     */
    [[nodiscard]] EventTime communication_delay(ChunkSize chunk_size) const noexcept;

    /**
     * Current congestion level seen by this link.
     * Defined as pending queue depth plus one when link is busy.
     */
    [[nodiscard]] uint64_t current_congestion_level() const noexcept;

    /**
     * Update time-weighted queue depth accounting until the given timestamp.
     */
    void update_queue_time_accounting(EventTime now) noexcept;

    /**
     * Return true when a chunk should be quantized at current queue depth.
     */
    [[nodiscard]] bool should_quantize(uint64_t queue_depth) const noexcept;

    /**
     * Compute effective chunk size after quantization policy.
     */
    [[nodiscard]] ChunkSize effective_chunk_size(ChunkSize original_size,
                           bool quantized) const noexcept;

    /**
     * Ensure quantization event log stream is initialized.
     */
    static void ensure_quantization_event_stream() noexcept;

    /**
     * Emit one quantization event record for a transmission.
     */
    void log_quantization_event(const Chunk& chunk,
                  ChunkSize original_size,
                  ChunkSize modeled_size,
                  bool quantized,
                  uint64_t congestion_level,
                  EventTime current_time) noexcept;

    /**
     * Schedule the transmission of a chunk.
     * - Set the link as busy.
     * - Link becomes free after the serialization delay.
     * - Chunk arrives next node after the communication delay.
     *
     * @param chunk chunk to be transmitted
     */
    void schedule_chunk_transmission(std::unique_ptr<Chunk> chunk) noexcept;
};

}  // namespace NetworkAnalyticalCongestionAware
