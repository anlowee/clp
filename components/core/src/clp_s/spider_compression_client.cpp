#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <spider/client/spider.hpp>
#include <string>
#include <type_traits>
#include <utility>

#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "spider_tasks.hpp"

class InputFileIterator {
public:
    explicit InputFileIterator(std::string const& path) : m_stream(path) {}

    bool get_next_line(std::string& line) {
        if (false == m_stream.is_open()) {
            return false;
        }

        if (!std::getline(m_stream, line)) {
            m_stream.close();
        }
        if (line.empty()) {
            m_stream.close();
            return false;
        }
        return true;
    }

    bool done() { return false == m_stream.is_open(); }

private:
    std::ifstream m_stream;
};

std::vector<std::string> get_ingestion_urls(std::string const& input_path) {
    std::vector<std::string> ingestion_urls;
    InputFileIterator it{input_path};
    std::string url;
    while (it.get_next_line(url)) {
        ingestion_urls.emplace_back(std::move(url));
    }
    return ingestion_urls;
}

// NOLINTBEGIN(bugprone-exception-escape)
auto main(int argc, char const* argv[]) -> int {
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    // Parse the storage backend URL from the command line arguments
    if (argc != 2) {
        std::cerr << "Usage: ./client <storage-backend-url>\n";
        return 1;
    }
    const std::string storage_url{argv[1]};

    // Create a driver that connects to the Spider cluster
    spider::Driver driver{storage_url};

    // Submit tasks for execution
    std::vector<spider::Job<terrablob::CompressResult>> jobs;

    const std::string timestamp_key{"ts"};
    const std::string topic_name{"merchant_reporting-datalake"};
    const std::string archives_suffix{topic_name};
    const std::string destination_prefix{fmt::format("{}/{}-test", DEBUG_PATH_PREFIX, topic_name)};

    constexpr size_t cBatchSize{5};
    // When the size is cBatchSize, it will assign to a task
    std::vector<std::string> s3_paths;
    // The last listed file in each listing operation is used as the marker for the next listing
    std::optional<std::string> marker{};
    // The raw data files that have been compressed into the SFA uploaded to the terrablob
    auto successful_ingested_file_paths_set = std::unordered_set<std::string>();
    // The raw data files that failed to be compressed or the SFA failed to be uploaded to the terrablob
    auto failed_ingested_file_paths_set = std::unordered_set<std::string>();
    while (true) {
        auto parsed_file_info = terrablob::MerchantReporingDatalakeMaster::get_and_parse_listed_files_xml(marker);
        for (auto const& it : parsed_file_info) {
            if (std::string::npos != it.terrablob_path_str.find(MERCHANT_REPORTING_DATALAKE_TERRABLOB_PATH_PREFIX)
                && it.terrablob_path_str.ends_with(".clp.zst")) {
                const auto s3_path = fmt::format("{}{}", LOCAL_CERBERUS_PREFIX, it.terrablob_path_str);
                if (successful_ingested_file_paths_set.end() != successful_ingested_file_paths_set.find(s3_path)) {
                    continue;
                }
                // TODO: Maybe give the failed raw data files second (or N-th) chance to retry
                if (failed_ingested_file_paths_set.end() != failed_ingested_file_paths_set.find(s3_path)) {
                    continue;
                }
                s3_paths.emplace_back(s3_path);
                if (cBatchSize <= s3_paths.size()) {
                    jobs.emplace_back(driver.start(&compress, std::move(s3_paths), timestamp_key, archives_suffix, destination_prefix));
                    s3_paths.clear();
                }
            }
        }
        marker = parsed_file_info[parsed_file_info.size() - 1].terrablob_path_str;

        // Wait for the jobs to complete
        bool failed = false;
        for (auto& job : jobs) {
            job.wait_complete();
            // Handle the job's success/failure
            switch (auto job_status = job.get_status()) {
                case spider::JobStatus::Succeeded: {
                    const auto job_result = job.get_result();
                    successful_ingested_file_paths_set.insert(job_result.successful_path_strs.begin(), job_result.successful_path_strs.end());
                    failed_ingested_file_paths_set.insert(job_result.failed_path_strs.begin(), job_result.failed_path_strs.end());
                    // TODO: remove this, because we want to save it in file and upload/download to/from terrablob
                    SPDLOG_INFO("So far successful: {}, failed: {}", successful_ingested_file_paths_set.size(), failed_ingested_file_paths_set.size());
                    break;
                }
                case spider::JobStatus::Failed: {
                    std::pair<std::string, std::string> const error_and_fn_name = job.get_error();
                    SPDLOG_ERROR("Job failed in function {}-{}", error_and_fn_name.second, error_and_fn_name.first);
                    failed = true;
                    break;
                }
                default: {
                    SPDLOG_ERROR("Job is in unexpected state - {}", static_cast<std::underlying_type_t<decltype(job_status)>>(job_status));
                    failed = true;
                    break;
                }
            }
        }
        if (failed) {
            SPDLOG_ERROR("Error occurred in at least on job(s)");
        }
    }

    return 0;
}

// NOLINTEND(bugprone-exception-escape)
