/****************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "common/NetworkParser.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>

using namespace NetworkAnalytical;

NetworkParser::NetworkParser(const std::string& path) noexcept
        : dims_count(-1),
            quantization_enabled(false),
            quantization_ratio(1.0),
            quantization_queue_threshold(std::numeric_limits<uint64_t>::max()),
            quantization_policy("threshold"),
            quantization_metric("queue_depth"),
            quantization_threshold(static_cast<double>(std::numeric_limits<uint64_t>::max())),
            quantization_metadata_mode("none"),
            quantization_metadata_bytes_per_chunk(0),
            quantization_metadata_bytes_per_group(0),
            quantization_metadata_group_size_bytes(0) {
    npus_count_per_dim = {};
    bandwidth_per_dim = {};
    latency_per_dim = {};
    topology_per_dim = {};

    try {
        const auto network_config = YAML::LoadFile(path);
        parse_network_config_yml(network_config);
    } catch (const YAML::BadFile& e) {
        std::cerr << "[Error] (network/analytical) " << e.what() << std::endl;
        std::exit(-1);
    }
}

int NetworkParser::get_dims_count() const noexcept {
    assert(dims_count > 0);
    return dims_count;
}

std::vector<int> NetworkParser::get_npus_counts_per_dim() const noexcept {
    assert(dims_count > 0);
    assert(npus_count_per_dim.size() == dims_count);
    return npus_count_per_dim;
}

std::vector<Bandwidth> NetworkParser::get_bandwidths_per_dim() const noexcept {
    assert(dims_count > 0);
    assert(bandwidth_per_dim.size() == dims_count);
    return bandwidth_per_dim;
}

std::vector<Latency> NetworkParser::get_latencies_per_dim() const noexcept {
    assert(dims_count > 0);
    assert(latency_per_dim.size() == dims_count);
    return latency_per_dim;
}

std::vector<TopologyBuildingBlock> NetworkParser::get_topologies_per_dim() const noexcept {
    assert(dims_count > 0);
    assert(topology_per_dim.size() == dims_count);
    return topology_per_dim;
}

bool NetworkParser::get_quantization_enabled() const noexcept {
    return quantization_enabled;
}

double NetworkParser::get_quantization_ratio() const noexcept {
    return quantization_ratio;
}

uint64_t NetworkParser::get_quantization_queue_threshold() const noexcept {
    return quantization_queue_threshold;
}

std::string NetworkParser::get_quantization_policy() const noexcept {
    return quantization_policy;
}

std::string NetworkParser::get_quantization_metric() const noexcept {
    return quantization_metric;
}

double NetworkParser::get_quantization_threshold() const noexcept {
    return quantization_threshold;
}

std::string NetworkParser::get_quantization_metadata_mode() const noexcept {
    return quantization_metadata_mode;
}

uint64_t NetworkParser::get_quantization_metadata_bytes_per_chunk() const noexcept {
    return quantization_metadata_bytes_per_chunk;
}

uint64_t NetworkParser::get_quantization_metadata_bytes_per_group() const noexcept {
    return quantization_metadata_bytes_per_group;
}

uint64_t NetworkParser::get_quantization_metadata_group_size_bytes() const noexcept {
    return quantization_metadata_group_size_bytes;
}

void NetworkParser::parse_network_config_yml(const YAML::Node& network_config) noexcept {
    const auto topology_names = parse_vector<std::string>(network_config["topology"]);
    for (const auto& topology_name : topology_names) {
        topology_per_dim.push_back(NetworkParser::parse_topology_name(topology_name));
    }

    dims_count = static_cast<int>(topology_per_dim.size());
    npus_count_per_dim = parse_vector<int>(network_config["npus_count"]);
    bandwidth_per_dim = parse_vector<Bandwidth>(network_config["bandwidth"]);
    latency_per_dim = parse_vector<Latency>(network_config["latency"]);

    if (network_config["quantization"]) {
        const auto quantization = network_config["quantization"];
        if (quantization["enabled"]) {
            quantization_enabled = quantization["enabled"].as<bool>();
        }
        if (quantization["ratio"]) {
            quantization_ratio = quantization["ratio"].as<double>();
        }
        if (quantization["queue_threshold"]) {
            quantization_queue_threshold = quantization["queue_threshold"].as<uint64_t>();
        }
        if (quantization["policy"]) {
            quantization_policy = quantization["policy"].as<std::string>();
        }
        if (quantization["congestion_metric"]) {
            quantization_metric = quantization["congestion_metric"].as<std::string>();
        }
        if (quantization["threshold"]) {
            quantization_threshold = quantization["threshold"].as<double>();
        } else if (quantization["queue_threshold"]) {
            quantization_threshold = static_cast<double>(quantization_queue_threshold);
        }
        if (quantization["metadata"]) {
            const auto metadata = quantization["metadata"];
            if (metadata["mode"]) {
                quantization_metadata_mode = metadata["mode"].as<std::string>();
            }
            if (metadata["bytes_per_chunk"]) {
                quantization_metadata_bytes_per_chunk = metadata["bytes_per_chunk"].as<uint64_t>();
            }
            if (metadata["bytes_per_group"]) {
                quantization_metadata_bytes_per_group = metadata["bytes_per_group"].as<uint64_t>();
            }
            if (metadata["group_size_bytes"]) {
                quantization_metadata_group_size_bytes = metadata["group_size_bytes"].as<uint64_t>();
            }
        }
    }

    check_validity();
}

TopologyBuildingBlock NetworkParser::parse_topology_name(const std::string& topology_name) noexcept {
    assert(!topology_name.empty());
    if (topology_name == "Ring") {
        return TopologyBuildingBlock::Ring;
    }
    if (topology_name == "FullyConnected") {
        return TopologyBuildingBlock::FullyConnected;
    }
    if (topology_name == "Switch") {
        return TopologyBuildingBlock::Switch;
    }
    std::cerr << "[Error] (network/analytical) Topology name " << topology_name << " not supported" << std::endl;
    std::exit(-1);
}

void NetworkParser::check_validity() const noexcept {
    if (dims_count != npus_count_per_dim.size()) {
        std::cerr << "[Error] (network/analytical) length of npus_count (" << npus_count_per_dim.size()
                  << ") doesn't match with dimensions (" << dims_count << ")" << std::endl;
        std::exit(-1);
    }
    if (dims_count != bandwidth_per_dim.size()) {
        std::cerr << "[Error] (network/analytical) length of bandwidth (" << bandwidth_per_dim.size()
                  << ") doesn't match with dims_count (" << dims_count << ")" << std::endl;
        std::exit(-1);
    }
    if (dims_count != latency_per_dim.size()) {
        std::cerr << "[Error] (network/analytical) length of latency (" << latency_per_dim.size()
                  << ") doesn't match with dims_count (" << dims_count << ")" << std::endl;
        std::exit(-1);
    }
    for (const auto& npus_count : npus_count_per_dim) {
        if (npus_count <= 1) {
            std::cerr << "[Error] (network/analytical) npus_count (" << npus_count << ") should be larger than 1"
                      << std::endl;
            std::exit(-1);
        }
    }
    for (const auto& bandwidth : bandwidth_per_dim) {
        if (bandwidth <= 0) {
            std::cerr << "[Error] (network/analytical) bandwidth (" << bandwidth << ") should be larger than 0"
                      << std::endl;
            std::exit(-1);
        }
    }
    for (const auto& latency : latency_per_dim) {
        if (latency < 0) {
            std::cerr << "[Error] (network/analytical) latency (" << latency << ") should be non-negative"
                      << std::endl;
            std::exit(-1);
        }
    }
    if (!(quantization_ratio > 0.0 && quantization_ratio <= 1.0)) {
        std::cerr << "[Error] (network/analytical) quantization ratio (" << quantization_ratio
                  << ") should be in (0, 1]" << std::endl;
        std::exit(-1);
    }
    if (!(quantization_policy == "threshold" || quantization_policy == "adaptive")) {
        std::cerr << "[Error] (network/analytical) quantization policy (" << quantization_policy
                  << ") should be one of: threshold, adaptive" << std::endl;
        std::exit(-1);
    }
    if (!(quantization_metric == "queue_depth" || quantization_metric == "queue_bytes" ||
          quantization_metric == "queue_delay" || quantization_metric == "utilization")) {
        std::cerr << "[Error] (network/analytical) quantization congestion metric (" << quantization_metric
                  << ") should be one of: queue_depth, queue_bytes, queue_delay, utilization" << std::endl;
        std::exit(-1);
    }
    if (!(std::isfinite(quantization_threshold) && quantization_threshold >= 0.0)) {
        std::cerr << "[Error] (network/analytical) quantization threshold (" << quantization_threshold
                  << ") should be finite and non-negative" << std::endl;
        std::exit(-1);
    }
    if (!(quantization_metadata_mode == "none" || quantization_metadata_mode == "per_tensor" ||
          quantization_metadata_mode == "per_group")) {
        std::cerr << "[Error] (network/analytical) quantization metadata mode (" << quantization_metadata_mode
                  << ") should be one of: none, per_tensor, per_group" << std::endl;
        std::exit(-1);
    }
    if (quantization_metadata_mode == "per_group" && quantization_metadata_group_size_bytes == 0) {
        std::cerr << "[Error] (network/analytical) quantization.metadata.group_size_bytes should be > 0 for per_group mode"
                  << std::endl;
        std::exit(-1);
    }
}
