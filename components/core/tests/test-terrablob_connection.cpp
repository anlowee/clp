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
#include "pugixml.hpp"
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <unordered_set>
#include <memory>

#include "../src/clp_s/spider_tasks.hpp"

#include <archive.h>

/**
 * Test if the basic listing works (1K files by default)
 */
void test_listing_files() {
    auto parsed_file_info = terrablob::MerchantReporingDatalakeMaster::get_and_parse_listed_files_xml(std::optional<std::string>());
    size_t nr_valid_listed_files{0};
    for (auto const& it : parsed_file_info) {
        if (std::string::npos != it.terrablob_path_str.find(MERCHANT_REPORTING_DATALAKE_TERRABLOB_PATH_PREFIX)
            && 100 <= it.last_modified.tm_year
            && 0 <= it.last_modified.tm_mon && 11 >= it.last_modified.tm_mon
            && 1 <= it.last_modified.tm_mday && 31 >= it.last_modified.tm_mday
            && 0 <= it.last_modified.tm_hour && 24 >= it.last_modified.tm_hour
            && 0 <= it.last_modified.tm_min && 60 >= it.last_modified.tm_min
            && 0 <= it.last_modified.tm_sec && 60 >= it.last_modified.tm_sec)
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
        auto parsed_file_info = terrablob::MerchantReporingDatalakeMaster::get_and_parse_listed_files_xml(marker);
        for (auto const& it : parsed_file_info) {
            if (std::string::npos != it.terrablob_path_str.find(MERCHANT_REPORTING_DATALAKE_TERRABLOB_PATH_PREFIX)
                && it.terrablob_path_str.ends_with(".clp.zst")) {
                listed_files_paths_set.emplace(it.terrablob_path_str);
            }
        }
        marker = parsed_file_info[parsed_file_info.size() - 1].terrablob_path_str;
    }
    SPDLOG_INFO("test_listing_irv2_files_with_marker: pass");
}

void test_download_file() {
    const std::string downloaded_file_path_str{"/tmp/test-download.clp.zst"};
    const std::string test_file_path_str{fmt::format("{}/80bce2fc-1727372068523_587747_01_000006-1741159529422.clp.zst", MERCHANT_REPORTING_DATALAKE_TERRABLOB_PATH_PREFIX)};
    if (std::filesystem::exists(downloaded_file_path_str)) {
        SPDLOG_INFO("test_download_file: test file is already downloaded, remove it before testing");
        std::filesystem::remove(downloaded_file_path_str);   
    }
    const auto downloaded_file = terrablob::MerchantReporingDatalakeMaster::download_file_from_terrablob(test_file_path_str, downloaded_file_path_str);
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

void test_ingesting_from_terrablob() {
    bool is_pass{true};
    const std::string test_archives_suffix = "test";
    constexpr size_t cNrTestedIrv2Files{1};
    const std::string test_irv2_file_dir_path_str{"/tmp/test-irv2-files"};
    if (std::filesystem::exists(test_irv2_file_dir_path_str)) {
        SPDLOG_INFO("test_ingesting_from_terrablob: {} exists, delete it", test_irv2_file_dir_path_str);
        std::filesystem::remove_all(test_irv2_file_dir_path_str);
    }
    if (std::filesystem::create_directories(test_irv2_file_dir_path_str)) {
        SPDLOG_INFO("test_ingesting_from_terrablob: create {}", test_irv2_file_dir_path_str);
    } else {
        SPDLOG_ERROR("test_ingesting_from_terrablob: {} does not exist, but failed to create", test_irv2_file_dir_path_str);
        return;
    }

    auto raw_data_file_info = terrablob::MerchantReporingDatalakeMaster::get_and_parse_listed_files_xml(std::optional<std::string>{});
    std::vector<std::string> input_irv2_file_downloaded_path_strs{};
    for (const auto& it : raw_data_file_info) {
        if (cNrTestedIrv2Files <= input_irv2_file_downloaded_path_strs.size()) {
            break;
        }
        const auto input_irv2_file_name = std::filesystem::path(it.terrablob_path_str).stem().string();
        const auto downloaded_file_path_str = fmt::format("{}/{}", test_irv2_file_dir_path_str, input_irv2_file_name);
        const auto downloaded_file_path = terrablob::MerchantReporingDatalakeMaster::download_file_from_terrablob(it.terrablob_path_str, downloaded_file_path_str);
        if (false == downloaded_file_path.has_value()) {
            SPDLOG_ERROR("test_ingesting_from_terrablob: fail to download {} to {}", it.terrablob_path_str, downloaded_file_path_str);
            continue;
        }
        SPDLOG_INFO("test_ingesting_from_terrablob: successfully download {} to {}, file size: {}", it.terrablob_path_str, downloaded_file_path_str, std::filesystem::file_size(downloaded_file_path.value()));
        input_irv2_file_downloaded_path_strs.emplace_back(downloaded_file_path.value().string());
    }
    const auto compress_result = terrablob::MerchantReportingDatalakeIngester::ingest_from_local_and_get_successfully_file_paths(
        input_irv2_file_downloaded_path_strs,
        "ts",
        test_archives_suffix,
        fmt::format("{}/test", DEBUG_PATH_PREFIX));
   
    if (cNrTestedIrv2Files != compress_result.successful_path_strs.size()) {
        SPDLOG_ERROR("test_ingesting_from_terrablob: only {}/{} IRV2 files were ingested successfully", compress_result.successful_path_strs.size(), cNrTestedIrv2Files);
        is_pass = false;
    } else {
        for (const auto& successful_path : compress_result.successful_path_strs) {
            SPDLOG_INFO("test_ingesting_from_terrablob: raw data path: {}", successful_path);
        }
        SPDLOG_INFO("test_ingesting_from_terrablob: compression ratio: {} -> {}", compress_result.uncompressed_size, compress_result.archive_size);
        SPDLOG_INFO("test_ingesting_from_terrablob: uploaded archive path: {}", compress_result.archive_terrablob_path_str);
    }
    

    if (is_pass) {
        SPDLOG_INFO("test_ingesting_from_terrablob: pass");
    } else {
        SPDLOG_INFO("test_ingesting_from_terrablob: fail");
    }
    if (std::filesystem::exists(test_irv2_file_dir_path_str)) {
        SPDLOG_INFO("test_ingesting_from_terrablob: delete local downloaded IRV2 files");
        std::filesystem::remove_all(test_irv2_file_dir_path_str);
    }
}

void test_ingesting_directly_from_terrablob() {
    bool is_pass{true};
    const std::string test_archives_suffix = "test";
    constexpr size_t cNrTestedIrv2Files{1};

    std::vector<std::string> input_irv2_file_terrablob_path_strs{fmt::format("{}/prod/logging/athena/merchant-reporting-datalake/phx/80bce2fc-1727372068523_587747_01_000006-1741159529422.clp.zst", LOCAL_CERBERUS_PREFIX)};
    const auto compress_result = terrablob::MerchantReportingDatalakeIngester::ingest_from_terrablob_and_get_successfully_file_paths(
        input_irv2_file_terrablob_path_strs,
        "ts",
        test_archives_suffix,
        fmt::format("{}/test", DEBUG_PATH_PREFIX));
    if (cNrTestedIrv2Files != compress_result.successful_path_strs.size()) {
        SPDLOG_ERROR("test_ingesting_directly_from_terrablob: only {}/{} IRV2 files were ingested successfully", compress_result.successful_path_strs.size(), cNrTestedIrv2Files);
        is_pass = false;
    } else {
        for (const auto& successful_path : compress_result.successful_path_strs) {
            SPDLOG_INFO("test_ingesting_directly_from_terrablob: raw data path: {}", successful_path);
        }
        SPDLOG_INFO("test_ingesting_directly_from_terrablob: compression ratio: {} -> {}", compress_result.uncompressed_size, compress_result.archive_size);
        SPDLOG_INFO("test_ingesting_directly_from_terrablob: uploaded archive path: {}", compress_result.archive_terrablob_path_str);
    }

    if (is_pass) {
        SPDLOG_INFO("test_ingesting_directly_from_terrablob: pass");
    } else {
        SPDLOG_INFO("test_ingesting_directly_from_terrablob: fail");
    }
}

int main() {
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    // test_listing_files();
    // test_listing_irv2_files_with_marker();
    // test_download_file();
    // test_upload_file();
    // test_ingesting_from_terrablob();
    test_ingesting_directly_from_terrablob();


    return 0;
}