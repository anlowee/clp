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

#include "../clp/CurlEasyHandle.hpp"
#include "../clp/CurlGlobalInstance.hpp"
#include "../clp/aws/AwsAuthenticationSigner.hpp"
#include "ArchiveReader.hpp"
#include "CommandLineArguments.hpp"
#include "Defs.hpp"
#include "InputConfig.hpp"
#include "JsonParser.hpp"
#include "TimestampPattern.hpp"
#include "pugixml.hpp"

#include <archive.h>

namespace terrablob {

std::vector<RawDataFileMetaData> MerchantReporingDatalakeMaster::get_and_parse_listed_files_xml(const std::optional<std::string_view>& marker) {
    clp::CurlEasyHandle handle;
    std::string response;
    std::string url = fmt::format("{}/?prefix={}", LOCAL_CERBERUS_PREFIX, MERCHANT_REPORTING_DATALAKE_TERRABLOB_PATH_PREFIX);
    if (marker.has_value()) {
        url = url + "&marker=" + marker->data();
    }
    handle.set_option(CURLOPT_URL, url.c_str());
    handle.set_option(CURLOPT_HTTPGET, 1L);

    handle.set_option(CURLOPT_WRITEFUNCTION, write_curl_response_to_string_callback);
    handle.set_option(CURLOPT_WRITEDATA, &response);
    auto curl_code = handle.perform();
    if (CURLE_OK != curl_code) {
        SPDLOG_ERROR("get_and_parse_listed_files_xml: curl request failed");
        return std::vector<RawDataFileMetaData>{};
    }

    pugi::xml_document doc;
    auto result = doc.load_string(response.data());
    if (!result) {
        return std::vector<RawDataFileMetaData>{};
    }
    std::vector<RawDataFileMetaData> file_infos;
    const auto root = doc.child("ListBucketResult");
    for (auto const& content : root.children("Contents")) {
        const auto key = content.child("Key").text().as_string();
        const auto last_modified = parse_ISO8601_time(content.child("LastModified").text().as_string());
        const auto file_size = content.child("Size").text().as_ullong();
        const auto file_info = std::make_unique<RawDataFileMetaData>();
        file_info->terrablob_path_str = key;
        file_info->last_modified = last_modified;
        file_info->file_size = file_size;
        file_infos.emplace_back(*file_info);
    }
    return file_infos;
}

std::optional<std::filesystem::path> MerchantReporingDatalakeMaster::download_file_from_terrablob(std::string_view terrablob_path_str, std::string_view local_file_path_str) {
    clp::CurlEasyHandle handle;
    std::ofstream file(std::string(local_file_path_str), std::ios::binary);

    if (!file.is_open()) {
        SPDLOG_ERROR("download_file_from_terrablob: failed to open the downloaded file");
        return std::optional<std::filesystem::path>{};
    }

    const std::string url = LOCAL_CERBERUS_PREFIX + std::string(terrablob_path_str);
    handle.set_option(CURLOPT_URL, url.c_str());
    handle.set_option(CURLOPT_HTTPGET, 1L);
    handle.set_option(CURLOPT_WRITEFUNCTION, write_curl_response_to_file_callback);
    handle.set_option(CURLOPT_WRITEDATA, &file);

    auto curl_code = handle.perform();
    file.close();

    if (CURLE_OK != curl_code) {
        SPDLOG_ERROR("download_file_from_terrablob: curl request failed");
        return std::optional<std::filesystem::path>{};
    }
    return std::filesystem::path(local_file_path_str);
}

bool MerchantReportingDatalakeIngester::upload_file_to_terrablob(std::string_view local_file_path_str, std::string_view terrablob_path_str) {
    std::ifstream file(std::string(local_file_path_str), std::ios::binary);
    if (!file.is_open()) {
        SPDLOG_ERROR("upload_file_to_terrablob: failed to open file {}", local_file_path_str);
        return false;
    }

    std::uintmax_t file_size = std::filesystem::file_size(local_file_path_str);

    clp::CurlEasyHandle handle;
    const std::string url = LOCAL_CERBERUS_PREFIX + std::string(terrablob_path_str);
    handle.set_option(CURLOPT_URL, url.c_str());
    handle.set_option(CURLOPT_UPLOAD, 1L);
    handle.set_option(CURLOPT_READDATA, &file);
    handle.set_option(CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(file_size));
    handle.set_option(CURLOPT_READFUNCTION, read_from_local_file_callback);  // Read file in chunks

    auto curl_code = handle.perform();
    file.close();

    if (CURLE_OK != curl_code) {
        SPDLOG_ERROR("upload_file_to_terrablob: curl request failed");
        return false;
    }
    return true;
}

CompressResult MerchantReportingDatalakeIngester::ingest_from_local_and_get_successfully_file_paths(
    const std::vector<std::string>& input_path_strs,
    std::string_view timestamp_key,
    std::string_view archive_suffix,
    std::string_view destination_prefix) {
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
    return execute_clps_and_upload_archive(option, destination_prefix);
}

CompressResult MerchantReportingDatalakeIngester::ingest_from_terrablob_and_get_successfully_file_paths(
    const std::vector<std::string>& input_terrablob_path_strs,
    std::string_view timestamp_key,
    std::string_view archives_path_suffix,
    std::string_view destination_prefix) {
    clp_s::TimestampPattern::init();

    // Set up some AWS related OS env here
    const std::string topic_name = "merchant-reporting-datalake";
    const std::string aws_access_key_id_env = "AWS_ACCESS_KEY_ID";
    const std::string aws_secret_access_key_env = "AWS_SECRET_ACCESS_KEY";

    if (setenv(aws_access_key_id_env.c_str(), topic_name.c_str(), 1) != 0) {
        SPDLOG_ERROR("ingest_from_terrablob_and_get_successfully_file_paths: fail to set environment variable {} to {}", aws_access_key_id_env, topic_name);
        return CompressResult{ {}, {input_terrablob_path_strs} };
    } else {
        SPDLOG_INFO("ingest_from_terrablob_and_get_successfully_file_paths: set environment variable {}={}", aws_access_key_id_env, getenv(aws_access_key_id_env.c_str()));
    }
    if (setenv(aws_secret_access_key_env.c_str(), "", 1) != 0) {
        SPDLOG_ERROR("ingest_from_terrablob_and_get_successfully_file_paths: fail to set environment variable {} to {}", aws_secret_access_key_env, topic_name);
        return CompressResult{ {}, {input_terrablob_path_strs} };
    } else {
        SPDLOG_INFO("ingest_from_terrablob_and_get_successfully_file_paths: set environment variable {}={}", aws_secret_access_key_env, getenv(aws_secret_access_key_env.c_str()));
    }

    clp_s::JsonParserOption option{};
    for (auto& path_str : input_terrablob_path_strs) {
        option.input_paths.emplace_back(
            clp_s::Path{.source = clp_s::InputSource::Network, .path = std::move(path_str)}
        );
    }

    option.input_file_type = clp_s::FileType::KeyValueIr;
    option.timestamp_key = timestamp_key;
    option.archives_dir = fmt::format("/tmp/archives-{}/", archives_path_suffix);
    option.target_encoded_size = 512 * 1024 * 1024;  // 512 MiB
    option.no_archive_split = true;  // So we force it only produce one SFA
    option.max_document_size = 512 * 1024 * 1024;  // 512 MiB
    option.min_table_size = 1 * 1024 * 1024;
    option.compression_level = 3;
    option.single_file_archive = true;
    option.network_auth = clp_s::NetworkAuthOption{.method = clp_s::AuthMethod::None};

    return execute_clps_and_upload_archive(option, destination_prefix);
}

CompressResult MerchantReportingDatalakeIngester::execute_clps_and_upload_archive(
    const clp_s::JsonParserOption &option,
    std::string_view destination_prefix) {
    const auto compress_result = std::make_unique<CompressResult>();
    std::vector<std::string> all_paths;
    try {
        std::filesystem::create_directory(option.archives_dir);
        clp_s::JsonParser parser{option};
        auto is_fully_success = parser.parse_from_ir();
        const auto successful_paths = parser.get_successfully_compressed_paths();
        const auto failed_paths = parser.get_unsuccessfully_compressed_paths();
        all_paths = successful_paths;
        all_paths.insert(all_paths.end(), failed_paths.begin(), failed_paths.end());
        if (false == is_fully_success && successful_paths.empty()) {
            cleanup_generated_archives(option.archives_dir);
            SPDLOG_ERROR("execute_clps_and_upload_archive: fail to compress all input paths.");
            return CompressResult{ {}, {all_paths} };

        }
        parser.store();
        size_t nr_valid_archives{0};
        std::string archive_name;
        std::string archive_local_path_str;
        size_t year, month, month_day;
        for (const auto& archive : std::filesystem::directory_iterator(option.archives_dir)) {
            const auto archive_info = get_archive_info_from_path(archive.path().string());
            if (false == archive_info.has_value()) {
                continue;
            }
            const auto archive_name_with_timestamp_range = fmt::format("{}/{}", archive.path().parent_path().string(), archive_info.value().archive_name_with_timestamp_range);
            std::filesystem::rename(archive, archive_name_with_timestamp_range);
            nr_valid_archives++;
            archive_name = archive_info->archive_name_with_timestamp_range;
            archive_local_path_str = archive_name_with_timestamp_range;
            year = archive_info->year;
            month = archive_info->month;
            month_day = archive_info->month_day;
            compress_result->uncompressed_size = archive_info->uncompressed_size;
            compress_result->archive_size = archive_info->archive_size;
        }
        if (1 != nr_valid_archives) {
            SPDLOG_ERROR("execute_clps_and_upload_archive: the archive is split");
            cleanup_generated_archives(option.archives_dir);
            return CompressResult{ {}, {all_paths} };
        }
        const auto archive_terrablob_path_str = fmt::format("{}/{}/{}/{}/{}", destination_prefix, year, month, month_day, archive_name);
        if (upload_file_to_terrablob(archive_local_path_str, archive_terrablob_path_str)) {
            compress_result->successful_path_strs = successful_paths;
            compress_result->archive_terrablob_path_str = archive_terrablob_path_str;
            SPDLOG_INFO("execute_clps_and_upload_archive: uploaded archive from {} to {}", archive_local_path_str, archive_terrablob_path_str);
            cleanup_generated_archives(option.archives_dir);
        } else {
            SPDLOG_ERROR("execute_clps_and_upload_archive: failed to upload archive from {} to {}", archive_local_path_str, archive_terrablob_path_str);
            cleanup_generated_archives(option.archives_dir);
            return CompressResult{ {}, {all_paths} };
        }
    } catch (std::exception const& e) {
        cleanup_generated_archives(option.archives_dir);
        SPDLOG_ERROR("execute_clps_and_upload_archive: encountered exception during ingestion - {}", e.what());
        return CompressResult{ {}, {all_paths} };
    }

    return *compress_result;
}


std::tm MerchantReporingDatalakeMaster::parse_ISO8601_time(const std::string& datetime) {
    std::tm time_struct = {};
    std::istringstream ss(datetime);
    ss >> std::get_time(&time_struct, "%Y-%m-%dT%H:%M:%SZ");

    if (ss.fail()) {
        throw std::runtime_error("Failed to parse datetime");
    }

    return time_struct;
}

size_t MerchantReporingDatalakeMaster::write_curl_response_to_string_callback(void* contents, size_t size, size_t nmemb, std::string* response) {
    size_t total_size = size * nmemb;
    response->append(static_cast<char*>(contents), total_size);
    return total_size;
}

size_t MerchantReporingDatalakeMaster::write_curl_response_to_file_callback(void* ptr, size_t size, size_t nmemb, void* stream) {
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

std::optional<ArchiveInfo> MerchantReportingDatalakeIngester::get_archive_info_from_path(std::string_view archive_path_str) {
    const auto archive_info = std::make_unique<ArchiveInfo>();
    std::filesystem::path archive_path{archive_path_str};
    archive_info->archive_size = std::filesystem::file_size(archive_path);
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
        struct tm time_info;
        time_t begin_timestamp_time{static_cast<time_t>(std::stoull(std::to_string(range->get_begin_timestamp())))};
        gmtime_r(&begin_timestamp_time, &time_info);
        archive_info->year = time_info.tm_year + 1900;
        archive_info->month = time_info.tm_mon + 1;
        archive_info->month_day = time_info.tm_mday;
    }
    archive_info->uncompressed_size = reader.get_archive_header().uncompressed_size;

    reader.close();
    archive_info->archive_name_with_timestamp_range = archive_name;
    // Sanity check
    if (archive_info->archive_name_with_timestamp_range.empty()
        || 0 >= archive_info->uncompressed_size
        || 0 >= archive_info->archive_size) {
        return std::nullopt;
    }

    return std::optional{*archive_info};
}

}  // namespace

// Task function implementation
std::string compress(
        spider::TaskContext& context,
        std::string s3_paths_json_str,
        std::string timestamp_key,
        std::string archives_suffix,
        std::string destination_prefix
) {
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    terrablob::InputPaths s3_paths = nlohmann::json::parse(s3_paths_json_str).get<terrablob::InputPaths>();
    nlohmann::json compress_result = terrablob::CompressResult{};

    const auto task_id_str = boost::uuids::to_string(context.get_id());
    SPDLOG_INFO("compress: task id: {}, number of input paths: {}, timestamp key: {}", task_id_str, s3_paths.input_paths.size(), timestamp_key);

    if (s3_paths.input_paths.empty()) {
        return compress_result.dump();
    }

    clp::CurlGlobalInstance const curl_global_instance;
    clp_s::TimestampPattern::init();

    const auto archives_suffix_with_task_id = fmt::format("{}-{}", archives_suffix, task_id_str);

    compress_result = terrablob::MerchantReportingDatalakeIngester::ingest_from_terrablob_and_get_successfully_file_paths(
        s3_paths.input_paths,
        timestamp_key,
        archives_suffix_with_task_id,
        destination_prefix
    );
    return compress_result.dump();
}

// Register the task with Spider
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(compress);
