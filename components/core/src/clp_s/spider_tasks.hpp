#ifndef TASKS_HPP
#define TASKS_HPP

#include "JsonParser.hpp"

#include <filesystem>
#include <spider/client/spider.hpp>
#include <string>
#include <vector>

#define LOCAL_CERBERUS_PREFIX "http://127.0.0.1:19617"
#define MERCHANT_REPORTING_DATALAKE_TERRABLOB_PATH_PREFIX "/prod/logging/athena/merchant-reporting-datalake/phx"
#define DEBUG_PATH_PREFIX "/prod/personal/xwei19/temp"

namespace terrablob {

typedef struct {
        std::string terrablob_path_str;
        std::tm last_modified;
        size_t file_size;
} RawDataFileMetaData;

typedef struct {
        std::string archive_name_with_timestamp_range;
        size_t year;
        size_t month;
        size_t month_day;
        size_t uncompressed_size;
        size_t archive_size;
} ArchiveInfo;

typedef struct InputPaths {
        std::vector<std::string> input_paths;

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(InputPaths, input_paths)
} InputPaths;

typedef struct CompressResult {
        std::vector<std::string> successful_path_strs;
        std::vector<std::string> failed_path_strs;
        std::string archive_terrablob_path_str;
        size_t uncompressed_size;
        size_t archive_size;

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(CompressResult,
                successful_path_strs, failed_path_strs, archive_terrablob_path_str,
                uncompressed_size, archive_size)
} CompressResult;

class MerchantReporingDatalakeMaster {
public:
        static std::vector<RawDataFileMetaData> get_and_parse_listed_files_xml(const std::optional<std::string_view>& marker);
        static std::optional<std::filesystem::path> download_file_from_terrablob(std::string_view terrablob_path_str, std::string_view local_file_path_str);
private:
        static std::tm parse_ISO8601_time(const std::string& datetime);
        static size_t write_curl_response_to_string_callback(void* contents, size_t size, size_t nmemb, std::string* response);
        static size_t write_curl_response_to_file_callback(void* ptr, size_t size, size_t nmemb, void* stream);
};

class MerchantReportingDatalakeIngester {
public:
        static CompressResult ingest_from_local_and_get_successfully_file_paths(
                const std::vector<std::string>& input_path_strs,
                std::string_view timestamp_key,
                std::string_view archive_suffix,
                std::string_view destination_prefix);
        static CompressResult ingest_from_terrablob_and_get_successfully_file_paths(
                const std::vector<std::string>& input_terrablob_path_strs,
                std::string_view timestamp_key,
                std::string_view archives_path_suffix,
                std::string_view destination_prefix);

private:
        static bool upload_file_to_terrablob(std::string_view local_file_path_str, std::string_view terrablob_path_str);
        static size_t read_from_local_file_callback(void* ptr, size_t size, size_t nmemb, void* stream);

        static void cleanup_generated_archives(std::string_view archives_path_str);
        static std::optional<ArchiveInfo> get_archive_info_from_path(std::string_view archive_path_str);
        static CompressResult execute_clps_and_upload_archive(
                const clp_s::JsonParserOption& option,
                std::string_view destination_prefix);
};

}

// Task function prototype
/**
 * @param context
 * @param s3_paths_json_str the json string of the serialized vector of s3 object URLs
 * @param timestamp_key the metadata needed by clp-s
 * @param archives_suffix the SFAs will be temporally stored at /tmp/archives-{archives_suffix}-{task_id}/
 * @param destination_prefix the SFAs will be uploaded to {destination_prefix}/yyyy/mm/dd/{min_ts}-{max_ts}.clps on terrablob
 * @return The json string of the serialized compress result contains some useful information
 */
std::string compress(
        spider::TaskContext& context,
        std::string s3_paths_json_str,
        std::string timestamp_key,
        std::string archives_suffix,
        std::string destination_prefix
);

#endif  // TASKS_HPP
