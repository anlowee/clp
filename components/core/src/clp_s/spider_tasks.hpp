#ifndef TASKS_HPP
#define TASKS_HPP

#include <spider/client/spider.hpp>
#include <string>
#include <vector>
#include <filesystem>

namespace terrablob {

class MerchantReportingDatalakeIngester {
public:
        const std::string kLocalCerberusPrefix{"http://127.0.0.1:19617"};
        const std::string kMerchantReportingDatalakseTerrablobPathPrefix{"/prod/logging/athena/merchant-reporting-datalake/phx"};
        const std::string kDebugPathPrefix{"/prod/personal/xwei19/temp"};

        std::vector<std::pair<std::string, std::tm>> get_and_parse_listed_files_xml(std::optional<std::string_view> marker) const;
        std::optional<std::filesystem::path> download_file_from_terrablob(std::string_view source_url, std::string_view destination_url) const;
        bool upload_file_to_terrablob(std::string_view source_url, std::string_view destination_url) const;
        static std::vector<std::string> compress_and_get_successfully_file_paths(const std::vector<std::string>& input_path_strs, std::string_view timestamp_key, std::string_view archives_path_suffix);

private:
        static std::tm parse_ISO8601_time(const std::string& datetime);
        static size_t write_curl_response_to_string_callback(void* contents, size_t size, size_t nmemb, std::string* response);
        static size_t write_curl_response_to_file_callback(void* ptr, size_t size, size_t nmemb, void* stream);
        static size_t read_from_local_file_callback(void* ptr, size_t size, size_t nmemb, void* stream);

        static void cleanup_generated_archives(std::string_view archives_path_str);
        static std::string get_archive_name_of_timestamp_range_from_path(std::string_view archive_path_str);

};

}

// Task function prototype
/**
 * @param context
 * @param s3_paths vector of s3 object URLs
 * @param destination
 * @param timestamp_key
 * @return The list of paths that were ingested succesfully.
 */
std::vector<std::string> compress(
        spider::TaskContext& context,
        std::vector<std::string> s3_paths,
        std::string destination,
        std::string timestamp_key
);

#endif  // TASKS_HPP
