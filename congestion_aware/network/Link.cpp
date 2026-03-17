/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "congestion_aware/Link.h"
#include "common/NetworkFunction.h"
#include "congestion_aware/Chunk.h"
#include "congestion_aware/Device.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>

using namespace NetworkAnalytical;
using namespace NetworkAnalyticalCongestionAware;

// declaring static event_queue
std::shared_ptr<EventQueue> Link::event_queue;
bool Link::quantization_enabled = false;
double Link::quantization_ratio = 1.0;
uint64_t Link::quantization_queue_threshold = std::numeric_limits<uint64_t>::max();
std::vector<Link*> Link::all_links;
bool Link::quantization_reported = false;
std::ofstream Link::quantization_event_stream;
bool Link::quantization_event_header_written = false;

void Link::configure_quantization(const bool enabled,
                                  const double ratio,
                                  const uint64_t queue_threshold) noexcept {
    quantization_enabled = enabled;
    quantization_ratio = ratio;
    quantization_queue_threshold = queue_threshold;
    ensure_quantization_event_stream();
}

void Link::ensure_quantization_event_stream() noexcept {
    if (quantization_event_stream.is_open()) {
        return;
    }

    const char* path_env = std::getenv("ASTRA_SIM_QUANT_EVENT_LOG");
    const std::string path =
        (path_env == nullptr || std::string(path_env).empty())
            ? "results/phase3/quantization_events.csv"
            : std::string(path_env);

    std::error_code ec;
    const auto parent = std::filesystem::path(path).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, ec);
    }

    quantization_event_stream.open(path, std::ios::out | std::ios::trunc);
    if (!quantization_event_stream.is_open()) {
        return;
    }

    quantization_event_stream
        << "time_ns,link_src_device,link_dst_device,node_id,comm_src,comm_dst,comm_tag,original_bytes,effective_bytes,quantized,congestion_level,quant_ratio,queue_threshold\n";
    quantization_event_header_written = true;
}

void Link::log_quantization_event(const Chunk& chunk,
                                  const ChunkSize original_size,
                                  const ChunkSize modeled_size,
                                  const bool quantized,
                                  const uint64_t congestion_level,
                                  const EventTime current_time) noexcept {
    if (!quantization_event_stream.is_open() || !quantization_event_header_written) {
        return;
    }

    const auto src_device = chunk.current_device()->get_id();
    const auto dst_device = chunk.next_device()->get_id();
    quantization_event_stream << current_time << ',' << src_device << ','
                              << dst_device << ',' << chunk.get_node_id() << ','
                              << chunk.get_comm_src() << ','
                              << chunk.get_comm_dst() << ','
                              << chunk.get_comm_tag() << ',' << original_size
                              << ',' << modeled_size << ','
                              << (quantized ? 1 : 0) << ','
                              << congestion_level << ',' << quantization_ratio
                              << ',' << quantization_queue_threshold << '\n';
}

void Link::report_quantization_stats() noexcept {
    if (quantization_reported || event_queue == nullptr) {
        return;
    }

    quantization_reported = true;

    const auto now = event_queue->get_current_time();
    uint64_t total_links = 0;
    uint64_t total_chunks = 0;
    uint64_t total_quantized_chunks = 0;
    uint64_t total_original_bytes = 0;
    uint64_t total_effective_bytes = 0;
    uint64_t total_queue_samples = 0;
    uint64_t total_queue_samples_above_threshold = 0;
    uint64_t global_max_queue_depth = 0;
    EventTime total_time_above_threshold = 0;

    for (auto* const link : all_links) {
        if (link == nullptr) {
            continue;
        }

        link->update_queue_time_accounting(now);
        total_links++;
        total_chunks += link->transmitted_chunks;
        total_quantized_chunks += link->quantized_chunks;
        total_original_bytes += link->original_transmitted_bytes;
        total_effective_bytes += link->effective_transmitted_bytes;
        total_queue_samples += link->queue_depth_samples;
        total_queue_samples_above_threshold +=
            link->queue_depth_samples_above_threshold;
        total_time_above_threshold += link->time_above_threshold;
        global_max_queue_depth =
            std::max(global_max_queue_depth, link->max_queue_depth_observed);
    }

    const auto quantized_chunk_ratio =
        (total_chunks == 0) ? 0.0
                            : static_cast<double>(total_quantized_chunks) /
                                  static_cast<double>(total_chunks);
    const auto queue_above_threshold_ratio =
        (total_queue_samples == 0)
            ? 0.0
            : static_cast<double>(total_queue_samples_above_threshold) /
                  static_cast<double>(total_queue_samples);
    const auto byte_reduction_ratio =
        (total_original_bytes == 0)
            ? 0.0
            : 1.0 -
                  (static_cast<double>(total_effective_bytes) /
                   static_cast<double>(total_original_bytes));

    std::cout << "[quantization] enabled="
              << (quantization_enabled ? "true" : "false")
              << " ratio=" << quantization_ratio
              << " queue_threshold=" << quantization_queue_threshold
              << std::endl;
    std::cout << "[quantization] links=" << total_links
              << " chunks_total=" << total_chunks
              << " chunks_quantized=" << total_quantized_chunks
              << " quantized_chunk_ratio=" << quantized_chunk_ratio
              << std::endl;
    std::cout << "[quantization] bytes_original=" << total_original_bytes
              << " bytes_effective=" << total_effective_bytes
              << " byte_reduction_ratio=" << byte_reduction_ratio
              << std::endl;
    std::cout << "[quantization] queue_samples=" << total_queue_samples
              << " queue_samples_above_threshold="
              << total_queue_samples_above_threshold
              << " queue_above_threshold_ratio=" << queue_above_threshold_ratio
              << " max_queue_depth=" << global_max_queue_depth << std::endl;
    std::cout << "[quantization] time_above_threshold_ns="
              << total_time_above_threshold << std::endl;

    if (quantization_event_stream.is_open()) {
        quantization_event_stream.flush();
        quantization_event_stream.close();
    }
}

void Link::link_become_free(void* const link_ptr) noexcept {
    assert(link_ptr != nullptr);

    // cast to Link*
    auto* const link = static_cast<Link*>(link_ptr);

    // set link free
    link->set_free();

    // process pending chunks if one exist
    if (link->pending_chunk_exists()) {
        link->process_pending_transmission();
    }
}

void Link::set_event_queue(std::shared_ptr<EventQueue> event_queue_ptr) noexcept {
    assert(event_queue_ptr != nullptr);

    // set the event queue
    Link::event_queue = std::move(event_queue_ptr);
}

Link::Link(const Bandwidth bandwidth, const Latency latency) noexcept
    : bandwidth(bandwidth),
      latency(latency),
      pending_chunks(),
            busy(false),
            queue_depth_at_last_update(0),
            queue_depth_last_update_time(0),
            time_above_threshold(0),
            queue_depth_samples(0),
            queue_depth_samples_above_threshold(0),
            max_queue_depth_observed(0),
            transmitted_chunks(0),
            quantized_chunks(0),
            original_transmitted_bytes(0),
            effective_transmitted_bytes(0) {
    assert(bandwidth > 0);
    assert(latency >= 0);

    // convert bandwidth from GB/s to B/ns
    bandwidth_Bpns = bw_GBps_to_Bpns(bandwidth);

    all_links.push_back(this);
}

void Link::send(std::unique_ptr<Chunk> chunk) noexcept {
    assert(chunk != nullptr);
    assert(event_queue != nullptr);

    const auto now = event_queue->get_current_time();
    update_queue_time_accounting(now);

    if (busy) {
        // link is busy, add to pending chunks
        pending_chunks.push_back(std::move(chunk));
        queue_depth_at_last_update = current_congestion_level();
        max_queue_depth_observed =
            std::max(max_queue_depth_observed, queue_depth_at_last_update);
    } else {
        // service this chunk immediately
        schedule_chunk_transmission(std::move(chunk));
    }
}

void Link::process_pending_transmission() noexcept {
    // pending chunk should exist
    assert(pending_chunk_exists());
    assert(event_queue != nullptr);

    const auto now = event_queue->get_current_time();
    update_queue_time_accounting(now);

    // get chunk to process
    auto chunk = std::move(pending_chunks.front());
    pending_chunks.pop_front();
    queue_depth_at_last_update = current_congestion_level();

    // service this chunk
    schedule_chunk_transmission(std::move(chunk));
}

bool Link::pending_chunk_exists() const noexcept {
    // check pending chunks is not empty
    return !pending_chunks.empty();
}

void Link::set_busy() noexcept {
    // set busy to true
    busy = true;
}

void Link::set_free() noexcept {
    // set busy to false
    busy = false;
}

EventTime Link::serialization_delay(const ChunkSize chunk_size) const noexcept {
    assert(chunk_size > 0);

    // calculate serialization delay
    const auto delay = static_cast<Bandwidth>(chunk_size) / bandwidth_Bpns;

    // return serialization delay in EventTime type
    return static_cast<EventTime>(delay);
}

EventTime Link::communication_delay(const ChunkSize chunk_size) const noexcept {
    assert(chunk_size > 0);

    // calculate communication delay
    const auto delay = latency + (static_cast<Bandwidth>(chunk_size) / bandwidth_Bpns);

    // return communication delay in EventTime type
    return static_cast<EventTime>(delay);
}

uint64_t Link::current_congestion_level() const noexcept {
    return pending_chunks.size() + (busy ? 1 : 0);
}

void Link::update_queue_time_accounting(const EventTime now) noexcept {
    if (now < queue_depth_last_update_time) {
        queue_depth_last_update_time = now;
        return;
    }

    if (queue_depth_at_last_update >= quantization_queue_threshold) {
        time_above_threshold += (now - queue_depth_last_update_time);
    }
    queue_depth_last_update_time = now;
}

bool Link::should_quantize(const uint64_t queue_depth) const noexcept {
    if (!quantization_enabled) {
        return false;
    }

    return queue_depth >= quantization_queue_threshold;
}

ChunkSize Link::effective_chunk_size(const ChunkSize original_size,
                                     const bool quantized) const noexcept {
    if (!quantized || quantization_ratio >= 1.0) {
        return original_size;
    }

    const auto scaled =
        static_cast<ChunkSize>(std::floor(static_cast<double>(original_size) *
                                          quantization_ratio));
    return std::max<ChunkSize>(1, scaled);
}

void Link::schedule_chunk_transmission(std::unique_ptr<Chunk> chunk) noexcept {
    assert(chunk != nullptr);

    // link should be free
    assert(!busy);

    // set link busy
    set_busy();

    // get metadata
    const auto chunk_size = chunk->get_size();
    const auto queue_depth = current_congestion_level();
    const auto quantized = should_quantize(queue_depth);
    const auto modeled_size = effective_chunk_size(chunk_size, quantized);
    const auto current_time = Link::event_queue->get_current_time();

    log_quantization_event(*chunk, chunk_size, modeled_size, quantized,
                           queue_depth, current_time);

    queue_depth_samples++;
    if (queue_depth >= quantization_queue_threshold) {
        queue_depth_samples_above_threshold++;
    }
    max_queue_depth_observed = std::max(max_queue_depth_observed, queue_depth);
    queue_depth_at_last_update = queue_depth;
    transmitted_chunks++;
    original_transmitted_bytes += chunk_size;
    effective_transmitted_bytes += modeled_size;
    if (quantized && modeled_size < chunk_size) {
        quantized_chunks++;
    }

    // schedule chunk arrival event
    const auto communication_time = communication_delay(modeled_size);
    const auto chunk_arrival_time = current_time + communication_time;
    auto* const chunk_ptr = static_cast<void*>(chunk.release());
    Link::event_queue->schedule_event(chunk_arrival_time, Chunk::chunk_arrived_next_device, chunk_ptr);

    // schedule link free time
    const auto serialization_time = serialization_delay(modeled_size);
    const auto link_free_time = current_time + serialization_time;
    auto* const link_ptr = static_cast<void*>(this);
    Link::event_queue->schedule_event(link_free_time, link_become_free, link_ptr);
}
