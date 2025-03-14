#include "spider_tasks.hpp"

#include <stdio.h>
#include <sys/stat.h>

#include <exception>
#include <filesystem>
#include <fstream>
#include <spider/client/spider.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <set>

#include <boost/uuid/random_generator.hpp>
#include <curl/curl.h>
#include <fmt/format.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "../clp/aws/AwsAuthenticationSigner.hpp"
#include "../clp/CurlEasyHandle.hpp"
#include "../clp/CurlGlobalInstance.hpp"
#include "ArchiveReader.hpp"
#include "CommandLineArguments.hpp"
#include "Defs.hpp"
#include "InputConfig.hpp"
#include "JsonParser.hpp"
#include "TimestampPattern.hpp"
#include "Utils.hpp"
#include "pugixml.hpp"

namespace terrablob {

std::vector<std::pair<std::string, std::tm>> MerchantReportingDatalakeIngester::get_and_parse_listed_files_xml(std::optional<std::string_view> marker) const {
    auto file_info = std::vector<std::pair<std::string, std::tm>>();
    clp::CurlEasyHandle handle;
    std::string response;
    std::string url = kLocalCerberusPrefix + "/?prefix=" + kMerchantReportingDatalakseTerrablobPathPrefix;
    if (marker.has_value()) {
        url = url + "&marker=" + marker->data();
    }
    handle.set_option(CURLOPT_URL, url.c_str());
    handle.set_option(CURLOPT_HTTPGET, 1L);

    handle.set_option(CURLOPT_WRITEFUNCTION, write_curl_response_to_string_callback);
    handle.set_option(CURLOPT_WRITEDATA, &response);
    auto curl_code = handle.perform();
    if (CURLE_OK != curl_code) {
        SPDLOG_ERROR("get_and_parse_listed_files_xml: curl request failed, error code: {}", curl_code);
        return file_info;
    }

    pugi::xml_document doc;
    auto result = doc.load_string(response.data());
    if (!result) {
        return file_info;
    }
    const auto root = doc.child("ListBucketResult");
    for (auto const& content : root.children("Contents")) {
        const auto key = content.child("Key").text().as_string();
        const auto last_modified = parse_ISO8601_time(content.child("LastModified").text().as_string());
        file_info.emplace_back(key, last_modified);
    }
    return file_info;
}

std::optional<std::filesystem::path> MerchantReportingDatalakeIngester::download_file_from_terrablob(std::string_view source_url, std::string_view destination_url) const {
    clp::CurlEasyHandle handle;
    std::ofstream file(std::string(destination_url), std::ios::binary);

    if (!file.is_open()) {
        SPDLOG_ERROR("download_file_from_terrablob: Failed to open the downloaded file");
        return std::optional<std::filesystem::path>{};
    }

    const std::string url = kLocalCerberusPrefix + std::string(source_url);
    handle.set_option(CURLOPT_URL, url.c_str());
    handle.set_option(CURLOPT_HTTPGET, 1L);
    handle.set_option(CURLOPT_WRITEFUNCTION, write_curl_response_to_file_callback);
    handle.set_option(CURLOPT_WRITEDATA, &file);

    auto curl_code = handle.perform();
    file.close();

    if (CURLE_OK != curl_code) {
        SPDLOG_ERROR("download_file_from_terrablob: curl request failed, error code: {}", curl_code);
        return std::optional<std::filesystem::path>{};
    }
    return std::filesystem::path(destination_url);
}

bool MerchantReportingDatalakeIngester::upload_file_to_terrablob(std::string_view source_url, std::string_view destination_url) const {
    std::ifstream file(std::string(source_url), std::ios::binary);
    if (!file.is_open()) {
        SPDLOG_ERROR("upload_file_to_terrablob: failed to open file {}", source_url);
        return false;
    }

    std::uintmax_t file_size = std::filesystem::file_size(source_url);

    clp::CurlEasyHandle handle;
    const std::string url = kLocalCerberusPrefix + std::string(destination_url);
    handle.set_option(CURLOPT_URL, url.c_str());
    handle.set_option(CURLOPT_UPLOAD, 1L);
    handle.set_option(CURLOPT_READDATA, &file);
    handle.set_option(CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(file_size));
    handle.set_option(CURLOPT_READFUNCTION, read_from_local_file_callback);  // Read file in chunks

    auto curl_code = handle.perform();
    file.close();

    if (CURLE_OK != curl_code) {
        SPDLOG_ERROR("upload_file_to_terrablob: upload failed with curl code: {}", curl_code);
        return false;
    }
    return true;
}

std::vector<std::string> MerchantReportingDatalakeIngester::compress_and_get_successfully_file_paths(
    const std::vector<std::string>& input_path_strs,
    std::string_view timestamp_key,
    std::string_view archive_suffix) {
    clp_s::TimestampPattern::init();

    clp_s::JsonParserOption option{};
    for (auto& path_str : input_path_strs) {
        option.input_paths.emplace_back(
                clp_s::Path{.source = clp_s::InputSource::Filesystem, .path = std::move(path_str)}
        );
    }

    option.input_file_type = clp_s::FileType::KeyValueIr;
    option.timestamp_key = timestamp_key;
    option.archives_dir = fmt::format("/tmp/archives-{}/", archive_suffix);
    option.target_encoded_size = 512 * 1024 * 1024;  // 512 MiB
    option.no_archive_split = true;
    option.max_document_size = 512 * 1024 * 1024;  // 512 MiB
    option.min_table_size = 1 * 1024 * 1024;
    option.compression_level = 3;
    option.single_file_archive = true;
    option.network_auth = clp_s::NetworkAuthOption{};
    std::vector<std::string> successful_paths;
    try {
        std::filesystem::create_directory(option.archives_dir);
        clp_s::JsonParser parser{option};
        auto is_fully_success = parser.parse_from_ir();
        successful_paths = parser.get_successfully_compressed_paths();
        if (false == is_fully_success) {
            if (successful_paths.empty()) {
                cleanup_generated_archives(option.archives_dir);
                SPDLOG_ERROR("Failed to compress all input paths.");
                return std::vector<std::string>{};
            }
        }
        parser.store();
    } catch (std::exception const& e) {
        cleanup_generated_archives(option.archives_dir);
        SPDLOG_ERROR("Encountered exception during ingestion - {}", e.what());
        return std::vector<std::string>{};
    }

    return successful_paths;
}

std::tm MerchantReportingDatalakeIngester::parse_ISO8601_time(const std::string& datetime) {
    std::tm time_struct = {};
    std::istringstream ss(datetime);
    ss >> std::get_time(&time_struct, "%Y-%m-%dT%H:%M:%SZ");

    if (ss.fail()) {
        throw std::runtime_error("Failed to parse datetime");
    }

    return time_struct;
}

size_t MerchantReportingDatalakeIngester::write_curl_response_to_string_callback(void* contents, size_t size, size_t nmemb, std::string* response) {
    size_t total_size = size * nmemb;
    response->append((char*)contents, total_size);
    return total_size;
}

size_t MerchantReportingDatalakeIngester::write_curl_response_to_file_callback(void* ptr, size_t size, size_t nmemb, void* stream) {
    std::ofstream* file = static_cast<std::ofstream*>(stream);
    file->write(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

size_t MerchantReportingDatalakeIngester::read_from_local_file_callback(void* ptr, size_t size, size_t nmemb, void* stream) {
    std::ifstream* file = static_cast<std::ifstream*>(stream);
    if (!file->read(static_cast<char*>(ptr), size * nmemb)) {
        return file->gcount();  // Return number of bytes actually read
    }
    return size * nmemb;
}

void MerchantReportingDatalakeIngester::cleanup_generated_archives(std::string_view archives_path_str) {
    std::error_code ec;
    std::filesystem::remove_all(std::filesystem::path(archives_path_str), ec);
    if (ec) {
        SPDLOG_ERROR("Failed to clean up archives path: ({}) {}", ec.value(), ec.message());
    }
}

std::string MerchantReportingDatalakeIngester::get_archive_name_of_timestamp_range_from_path(std::string_view archive_path_str) {
    std::filesystem::path archive_path{archive_path_str};
    // Extract timestamp range metadata by simply reading archive metadata
    std::string archive_name = "0-" + std::to_string(clp_s::cEpochTimeMax) + ".clps";
    clp_s::ArchiveReader reader;
    auto path
            = clp_s::Path{.source = clp_s::InputSource::Filesystem, .path = archive_path.string()};
    reader.open(path, clp_s::NetworkAuthOption{});
    auto timestamp_dict = reader.get_timestamp_dictionary();
    auto it = timestamp_dict->tokenized_column_to_range_begin();
    if (timestamp_dict->tokenized_column_to_range_end() != it) {
        auto range = it->second;
        archive_name = std::to_string(range->get_begin_timestamp()) + "-" + std::to_string(range->get_end_timestamp()) + ".clps";
    }
    reader.close();
    return archive_name;
}

}  // namespace

// Task function implementation
std::vector<std::string> compress(
        spider::TaskContext& context,
        std::vector<std::string> s3_paths,
        std::string destination,
        std::string timestamp_key
) {
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    if (s3_paths.empty()) {
        return std::vector<std::string>{};
    }

    clp::CurlGlobalInstance const curl_global_instance;
    clp_s::TimestampPattern::init();

    clp_s::JsonParserOption option{};
    for (auto& path : s3_paths) {
        option.input_paths.emplace_back(
                clp_s::Path{.source = clp_s::InputSource::Network, .path = std::move(path)}
        );
    }

    option.input_file_type = clp_s::FileType::KeyValueIr;
    option.timestamp_key = timestamp_key;
    option.archives_dir = fmt::format("/tmp/{}/", boost::uuids::to_string(context.get_id()));
    option.target_encoded_size = 512 * 1024 * 1024;  // 512 MiB
    option.no_archive_split = true;
    option.max_document_size = 512 * 1024 * 1024;  // 512 MiB
    option.min_table_size = 1 * 1024 * 1024;
    option.compression_level = 3;
    option.single_file_archive = true;
    option.network_auth = clp_s::NetworkAuthOption{.method = clp_s::AuthMethod::S3PresignedUrlV4};
    std::vector<std::string> successful_paths;
    try {
        std::filesystem::create_directory(option.archives_dir);
        clp_s::JsonParser parser{option};
        if (false == parser.parse_from_ir()) {
            successful_paths = parser.get_successfully_compressed_paths();
            if (successful_paths.empty()) {
                //cleanup_generated_archives(option.archives_dir);
                SPDLOG_ERROR("Failed to compress all input paths.");
                return std::vector<std::string>{};
            }
        }
        parser.store();
        // trigger upload
        // if (false == upload_all_files_in_directory(option.archives_dir, destination)) {
        //     cleanup_generated_archives(option.archives_dir);
        //     SPDLOG_ERROR("Encountered error during upload.");
        //     return std::vector<std::string>{};
        // }
    } catch (std::exception const& e) {
        //cleanup_generated_archives(option.archives_dir);
        SPDLOG_ERROR("Encountered exception during ingestion - {}", e.what());
        return std::vector<std::string>{};
    }

    //cleanup_generated_archives(option.archives_dir);
    return successful_paths;
}

// Register the task with Spider
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(compress);
