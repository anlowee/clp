//
// Created by xwei19 on 3/12/25.
//
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <vector>
#include <curl/curl.h>
#include "../src/clp/CurlEasyHandle.hpp"
#include "pugixml.hpp"
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <unordered_set>
#include <memory>

#include "../src/clp_s/spider_tasks.hpp"

#include <archive.h>

auto merchant_reporting_datalake_ingester = std::make_unique<terrablob::MerchantReportingDatalakeIngester>();

/**
 * Test if the basic listing works (1K files by default)
 */
void test_listing_files() {
    auto parsed_file_info = merchant_reporting_datalake_ingester->get_and_parse_listed_files_xml(std::optional<std::string>());
    size_t nr_valid_listed_files{0};
    for (auto const& it : parsed_file_info) {
        if (std::string::npos != it.first.find(merchant_reporting_datalake_ingester->kMerchantReportingDatalakseTerrablobPathPrefix)
            && 100 <= it.second.tm_year
            && 0 <= it.second.tm_mon && 11 >= it.second.tm_mon
            && 1 <= it.second.tm_mday && 31 >= it.second.tm_mday
            && 0 <= it.second.tm_hour && 24 >= it.second.tm_hour
            && 0 <= it.second.tm_min && 60 >= it.second.tm_min
            && 0 <= it.second.tm_sec && 60 >= it.second.tm_sec)
        nr_valid_listed_files++;
    }
    if (1000 != nr_valid_listed_files) {
        SPDLOG_ERROR("test_listing_files: number of valid listed files is not 1000 but: {}", nr_valid_listed_files);
        return;
    }
    SPDLOG_INFO("test_listing_files: pass");
}

/**
 * Test until it gets 10K different files
 */
void test_listing_irv2_files_with_marker() {
    auto listed_files_paths_set = std::unordered_set<std::string>();
    std::optional<std::string> marker{};
    while (listed_files_paths_set.size() < 10000) {
        auto parsed_file_info = merchant_reporting_datalake_ingester->get_and_parse_listed_files_xml(marker);
        for (auto const& it : parsed_file_info) {
            if (std::string::npos != it.first.find(merchant_reporting_datalake_ingester->kMerchantReportingDatalakseTerrablobPathPrefix)
                && it.first.ends_with(".clp.zst")) {
                listed_files_paths_set.emplace(it.first);
            }
        }
        marker = parsed_file_info[parsed_file_info.size() - 1].first;
    }
    SPDLOG_INFO("test_listing_irv2_files_with_marker: pass");
}

void test_download_file() {
    const std::string downloaded_file_path_str{"/tmp/test-download.clp.zst"};
    const std::string test_file_path_str{merchant_reporting_datalake_ingester->kMerchantReportingDatalakseTerrablobPathPrefix + "/80bce2fc-1727372068523_587747_01_000006-1741159529422.clp.zst"};
    if (std::filesystem::exists(downloaded_file_path_str)) {
        SPDLOG_INFO("test_download_file: test file is already downloaded, remove it before testing");
        std::filesystem::remove(downloaded_file_path_str);   
    }
    const auto downloaded_file = merchant_reporting_datalake_ingester->download_file_from_terrablob(test_file_path_str, downloaded_file_path_str);
    if (false == downloaded_file.has_value()) {
        SPDLOG_ERROR("test_download_file: failed to download file");
    } else {
        std::uintmax_t file_size = std::filesystem::file_size(downloaded_file.value());
        if (80825 != file_size) {
            SPDLOG_ERROR("test_download_file: downloaded file is broken, size: {}", file_size);
        } else {
            SPDLOG_INFO("test_download_file: pass");   
        }
        SPDLOG_INFO("test_download_file: delete downloaded test file");
        std::filesystem::remove(downloaded_file.value());
    }
}

void test_upload_file() {
    const std::string local_test_file_path_str{"/tmp/test-upload.clp.zst"};
    if (std::filesystem::exists(local_test_file_path_str)) {
        SPDLOG_INFO("test_upload_file: test file is already there, remove it before testing");
        std::filesystem::remove(local_test_file_path_str);
    }
    std::ofstream local_test_file_os(local_test_file_path_str);
    if (!local_test_file_os) {
        SPDLOG_ERROR("test_upload_file: failed to create the file to upload");
        return;
    }
    local_test_file_os << "test";
    local_test_file_os.close();
    const std::string uploaded_test_file_path_str{merchant_reporting_datalake_ingester->kDebugPathPrefix + "/test-upload.clp.zst"};
    if (merchant_reporting_datalake_ingester->upload_file_to_terrablob(local_test_file_path_str, uploaded_test_file_path_str)) {
        SPDLOG_INFO("test_upload_file: pass");
    } else {
        SPDLOG_ERROR("test_upload_file: failed to upload");
    }
    SPDLOG_INFO("test_upload_file: delete the local test file");
    std::filesystem::remove(local_test_file_path_str);
}

void test_ingesting_from_terrablob() {
    bool is_pass{true};
    constexpr size_t cNrTestedIrv2Files{10};
    const std::string test_irv2_file_dir_path_str{"/tmp/test-irv2-files"};
    if (std::filesystem::exists(test_irv2_file_dir_path_str)) {
        SPDLOG_INFO("test_ingesting_from_terrablob: {} exists, delete it", test_irv2_file_dir_path_str);
        std::filesystem::remove_all(test_irv2_file_dir_path_str);
    }
    if (std::filesystem::exists("/tmp/archives-test")) {
        SPDLOG_INFO("test_ingesting_from_terrablob: archives directory exists, delete it");
        std::filesystem::remove_all("/tmp/archives-test");
    }
    if (std::filesystem::create_directories(test_irv2_file_dir_path_str)) {
        SPDLOG_INFO("test_ingesting_from_terrablob: create {}", test_irv2_file_dir_path_str);
    } else {
        SPDLOG_ERROR("test_ingesting_from_terrablob: {} does not exist, but failed to create", test_irv2_file_dir_path_str);
        return;
    }

    auto raw_data_file_info = merchant_reporting_datalake_ingester->get_and_parse_listed_files_xml(std::optional<std::string>{});
    std::vector<std::string> input_irv2_file_downloaded_path_strs{};
    boost::uuids::random_generator generator;
    for (const auto& it : raw_data_file_info) {
        if (cNrTestedIrv2Files <= input_irv2_file_downloaded_path_strs.size()) {
            break;
        }
        const auto downloaded_file_path_str = fmt::format("{}/{}", test_irv2_file_dir_path_str, boost::uuids::to_string(generator()));
        const auto downloaded_file_path = merchant_reporting_datalake_ingester->download_file_from_terrablob(it.first, downloaded_file_path_str);
        if (false == downloaded_file_path.has_value()) {
            SPDLOG_ERROR("test_ingesting_from_terrablob: fail to download {} to {}", it.first, downloaded_file_path_str);
            continue;
        }
        SPDLOG_INFO("test_ingesting_from_terrablob: successfully download {} to {}, file size: {}", it.first, downloaded_file_path_str, std::filesystem::file_size(downloaded_file_path.value()));
        input_irv2_file_downloaded_path_strs.emplace_back(downloaded_file_path.value().string());
    }
    const auto successful_irv2_file_path_strs = merchant_reporting_datalake_ingester->compress_and_get_successfully_file_paths(input_irv2_file_downloaded_path_strs, "", "test");
    if (cNrTestedIrv2Files != successful_irv2_file_path_strs.size()) {
        SPDLOG_ERROR("test_ingesting_from_terrablob: only {}/{} IRV2 files were ingested successfully", successful_irv2_file_path_strs.size(), cNrTestedIrv2Files);
        is_pass = false;
    }
    const std::string upload_archive_terrablob_path_str{merchant_reporting_datalake_ingester->kDebugPathPrefix + "/test-ingestion.clps"};
    for (const auto& archive : std::filesystem::directory_iterator("/tmp/archives-test")) {
        SPDLOG_INFO("test_ingesting_from_terrablob: found archive: {}, size: {}", archive.path().string(), std::filesystem::file_size(archive.path()));
        if (merchant_reporting_datalake_ingester->upload_file_to_terrablob(archive.path().string(), upload_archive_terrablob_path_str)) {
            SPDLOG_INFO("test_ingesting_from_terrablob: successfully upload {} to {}", archive.path().string(), upload_archive_terrablob_path_str);
        } else {
            SPDLOG_ERROR("test_ingesting_from_terrablob: fail to upload {} to {}", archive.path().string(), upload_archive_terrablob_path_str);
            is_pass = false;
        }
    }

    if (is_pass) {
        SPDLOG_INFO("test_ingesting_from_terrablob: pass");
    } else {
        SPDLOG_INFO("test_ingesting_from_terrablob: fail");
    }
    SPDLOG_INFO("test_ingesting_from_terrablob: delete local downloaded IRV2 files");
    std::filesystem::remove_all(test_irv2_file_dir_path_str);
    SPDLOG_INFO("test_ingesting_from_terrablob: delete local archives directory");
    std::filesystem::remove_all("/tmp/archives-test");
}

int main() {
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    // test_listing_files();
    // test_listing_irv2_files_with_marker();
    // test_download_file();
    // test_upload_file();
    test_ingesting_from_terrablob();



    return 0;
}