#include "spider_tasks.hpp"

#include <stdio.h>
#include <sys/stat.h>

#include <exception>
#include <filesystem>
#include <spider/client/spider.hpp>
#include <stdexcept>
#include <string>
#include <vector>

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
#include "Utils.hpp"

namespace {
std::string get_upload_name_from_path(std::filesystem::path archive_path) {
    // Extract timestamp range metadata by simply reading archive metadata
    std::string postfix = "_0_" + std::to_string(clp_s::cEpochTimeMax);
    std::string archive_name = archive_path.stem().string();
    clp_s::ArchiveReader reader;
    auto path
            = clp_s::Path{.path = archive_path.string(), .source = clp_s::InputSource::Filesystem};
    reader.open(path, clp_s::NetworkAuthOption{});
    auto timestamp_dict = reader.get_timestamp_dictionary();
    auto it = timestamp_dict->tokenized_column_to_range_begin();
    if (timestamp_dict->tokenized_column_to_range_end() != it) {
        auto range = it->second;
        postfix = "_" + std::to_string(range->get_begin_timestamp()) + "_"
                  + std::to_string(range->get_end_timestamp());
    }
    reader.close();
    return archive_name + postfix;
}

bool upload_all_files_in_directory(std::string const& directory, std::string const& destination) {
    std::vector<std::string> file_paths;

    // Copy-pasted from ReaderUtils.cpp
    auto const aws_access_key = std::getenv(clp_s::cAwsAccessKeyIdEnvVar);
    auto const aws_secret_access_key = std::getenv(clp_s::cAwsSecretAccessKeyEnvVar);
    if (nullptr == aws_access_key || nullptr == aws_secret_access_key) {
        SPDLOG_ERROR(
                "{} and {} environment variables not available for presigned url authentication.",
                clp_s::cAwsAccessKeyIdEnvVar,
                clp_s::cAwsSecretAccessKeyEnvVar
        );
        return false;
    }
    std::optional<std::string> optional_aws_session_token{std::nullopt};
    auto const aws_session_token = std::getenv(clp_s::cAwsSessionTokenEnvVar);
    if (nullptr != aws_session_token) {
        optional_aws_session_token = std::string{aws_session_token};
    }

    clp_s::FileUtils::find_all_files_in_directory(directory, file_paths);
    clp::aws::AwsAuthenticationSigner signer(
            aws_access_key,
            aws_secret_access_key,
            optional_aws_session_token
    );
    for (auto const& path : file_paths) {
        FILE* fd = fopen(path.c_str(), "rb");
        struct stat file_info;
        if (nullptr == fd) {
            return false;
        }

        if (0 != fstat(fileno(fd), &file_info)) {
            fclose(fd);
            return false;
        }

        // Create unsigned url string
        std::string unsigned_url_str = destination;
        if (unsigned_url_str.empty()) {
            return false;
        }
        if (unsigned_url_str.back() != '/') {
            unsigned_url_str += '/';
        }
        unsigned_url_str += get_upload_name_from_path(std::filesystem::path(path));

        std::string presigned_url;
        try {
            clp::aws::S3Url s3_url(unsigned_url_str);
            if (auto rc = signer.generate_presigned_url(s3_url, presigned_url, false);
                clp::ErrorCode_Success != rc)
            {
                SPDLOG_ERROR("Failed to sign s3 url: rc={}", rc);
                return false;
            }
        } catch (std::exception const& e) {
            SPDLOG_ERROR(e.what());
            return false;
        }

        // Referencing https://curl.se/libcurl/c/fileupload.html example
        clp::CurlEasyHandle handle;
        handle.set_option(CURLOPT_URL, presigned_url.c_str());
        handle.set_option(CURLOPT_UPLOAD, 1L);
        handle.set_option(CURLOPT_READDATA, fd);
        handle.set_option(CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(file_info.st_size));
        auto curl_code = handle.perform();
        fclose(fd);
        if (CURLE_OK != curl_code) {
            SPDLOG_INFO("Upload failed with curl code: {}", static_cast<int>(curl_code));
            return false;
        }
    }
    return true;
}

void cleanup_generated_archives(std::string archives_path) {
    std::error_code ec;
    std::filesystem::remove_all(std::filesystem::path(archives_path), ec);
    if (ec) {
        SPDLOG_ERROR("Failed to clean up archives path: ({}) {}", ec.value(), ec.message());
    }
}
}  // namespace

// Task function implementation
int compress(
        spider::TaskContext& context,
        std::vector<std::string> s3_paths,
        std::string destination,
        std::string timestamp_key
) {
    auto stderr_logger = spdlog::stderr_logger_st("stderr");
    spdlog::set_default_logger(stderr_logger);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");

    clp::CurlGlobalInstance const curl_global_instance;

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
    option.max_document_size = 512 * 1024 * 1024;  // 512 MiB
    option.min_table_size = 1 * 1024 * 1024;
    option.compression_level = 3;
    option.single_file_archive = true;
    option.network_auth = clp_s::NetworkAuthOption{.method = clp_s::AuthMethod::S3PresignedUrlV4};
    try {
        std::filesystem::create_directory(option.archives_dir);
        clp_s::JsonParser parser{option};
        if (false == parser.parse_from_ir()) {
            throw std::runtime_error("Encountered error during parsing.");
            return 1;
        }
        parser.store();
        // trigger upload
        if (false == upload_all_files_in_directory(option.archives_dir, destination)) {
            throw std::runtime_error("Encountered error during upload.");
            return 1;
        }
    } catch (std::exception const& e) {
        cleanup_generated_archives(option.archives_dir);
        throw e;
        return 1;
    }

    cleanup_generated_archives(option.archives_dir);
    return 0;
}

// Register the task with Spider
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(compress);
