#include <filesystem>
#include <fstream>
#include <iostream>
#include <spider/client/spider.hpp>
#include <string>
#include <type_traits>
#include <utility>

#include "spider_tasks.hpp"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <spdlog/spdlog.h>

namespace terrablob {

class SawmillQueryRawDataHandler {
public:
    explicit SawmillQueryRawDataHandler() {
        // Download the ingested raw data file logs if not exists locally
        download_ingested_file_log_();
        std::ifstream ingested_file_log{kIngestedFileLogPathStr};
        if (!ingested_file_log) {
            SPDLOG_ERROR("Open log file failed");
            throw std::runtime_error("Open log file failed");
        }
        for (std::string line; std::getline(ingested_file_log, line);) {
            if (false == line.empty()) {
                m_ingested_raw_data_terrablob_paths.insert(line);
            }
        }
    }

  /**
   * Download and get the downloaded file path of the raw data (that were not
   * successfully ingested before) of the next date (but must be not the latest
   * date) of each region
   * @return A vector of downloaded raw data file paths
   */
    std::vector<std::filesystem::path> get_next();
    bool upload_ingested_file_log();

private:
    std::unordered_map<std::string, std::unordered_map<std::string, boost::posix_time::ptime>> m_current_region_dates{
        {"dca", {
             {"dca11", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 6), boost::posix_time::hours(23))},
             {"dca18", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 7), boost::posix_time::hours(4))},
             {"dca20", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 6), boost::posix_time::hours(18))},
             {"dca22", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 7), boost::posix_time::hours(6))},
             {"dca23", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 7), boost::posix_time::hours(10))},
             {"dca24", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 7), boost::posix_time::hours(15))},
             {"dca50", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 7), boost::posix_time::hours(17))},
             {"dca51", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 7), boost::posix_time::hours(20))}
        }},
        {"phx", {
             {"phx5",  boost::posix_time::ptime(boost::gregorian::date(2025, 2, 10), boost::posix_time::hours(11))},
             {"phx50", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 11), boost::posix_time::hours(16))},
             {"phx51", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 11), boost::posix_time::hours(19))},
             {"phx52", boost::posix_time::ptime(boost::gregorian::date(2025, 2, 11), boost::posix_time::hours(22))},
             {"phx6",  boost::posix_time::ptime(boost::gregorian::date(2025, 2, 10), boost::posix_time::hours(15))},
             {"phx7",  boost::posix_time::ptime(boost::gregorian::date(2025, 2, 10), boost::posix_time::hours(20))},
             {"phx8",  boost::posix_time::ptime(boost::gregorian::date(2025, 2, 11), boost::posix_time::hours(12))}
        }}
    };

    const std::string kPersonalTerrablobPathPrefix{"/prod/personal/xwei19/temp"};
    const std::string kTopicLoggingTerrablobPathPrefix = fmt::format("/prod/logging/warm_storage/{}", m_topic);
    const std::string kDownloadedRawDataPathPrefix = fmt::format("/tmp/{}-raw-data", m_topic);
    const std::string kIngestedFileLogFileName = fmt::format("{}-ingested-files.log", m_topic);
    const std::string kIngestedFileLogPathStr = fmt::format("/tmp/{}", kIngestedFileLogFileName);
    std::string m_topic{"sawmill-query"};
    std::unordered_set<std::string> m_ingested_raw_data_terrablob_paths;

    std::vector<std::string> get_tb_cli_ls_result_strings_(std::string_view terrablob_path_str);
    bool download_raw_data_from_terrablob_(std::string_view terrablob_path_str);
    void download_ingested_file_log_();
    std::string format_ptime_(boost::posix_time::ptime date);
    boost::posix_time::ptime parse_ptime_(const std::string &date_str);
};

std::string SawmillQueryRawDataHandler::format_ptime_(boost::posix_time::ptime date) {
    auto facet = std::make_unique<boost::posix_time::time_facet>("%Y-%m-%d-%H");
    std::ostringstream date_oss;
    date_oss.imbue(std::locale(std::locale::classic(), facet.get()));
    date_oss << date;
    return date_oss.str();
}

boost::posix_time::ptime SawmillQueryRawDataHandler::parse_ptime_(std::string const& date_str) {
    std::istringstream date_iss(date_str);
    // Imbue the stream with a locale that uses a time_input_facet for the desired format.
    date_iss.imbue(std::locale(std::locale::classic(), new boost::posix_time::time_input_facet("%Y-%m-%d-%H")));
    boost::posix_time::ptime pt;
    date_iss >> pt;
    return pt;
}

void SawmillQueryRawDataHandler::download_ingested_file_log_() {
    if (0 != system(fmt::format("tb-cli get {}/{} {}", kPersonalTerrablobPathPrefix, kIngestedFileLogFileName, kIngestedFileLogPathStr).c_str())) {
        std::ofstream ingested_file(kIngestedFileLogPathStr, std::ios::app);
        SPDLOG_INFO("The log file does not exist, create one locally: {}", kIngestedFileLogPathStr);
    } else {
        SPDLOG_INFO("Download log file {} successfully");
    }
}

bool SawmillQueryRawDataHandler::upload_ingested_file_log() {
    return 0 == system(fmt::format("tb-cli put -t 99999s {} {}/{}", kIngestedFileLogPathStr, kPersonalTerrablobPathPrefix, kIngestedFileLogFileName).c_str());
}

std::vector<std::filesystem::path> SawmillQueryRawDataHandler::get_next() {
    // Get all raw data files under the current date of each region, check if they were ingested
    std::vector<std::filesystem::path> next_batch_paths;
    for (auto& zone : m_current_region_dates) {
        for (auto& region_date : zone.second) {
            const std::string kTerrablobPathStrWithoutDate = fmt::format("{}/{}/{}", kTopicLoggingTerrablobPathPrefix, zone.first, region_date.first);
            const std::string kCurrentDate = format_ptime_(region_date.second);
            size_t next_batch_of_current_region_cnt = 0;
            // Download those are not ingested yet
            const std::string kTerrablobPathStrWithDate = fmt::format("{}/{}", kTerrablobPathStrWithoutDate, kCurrentDate);
            const std::vector<std::string> kRawDataFileNames = get_tb_cli_ls_result_strings_(kTerrablobPathStrWithDate);
            for (auto const& raw_data_file_name : kRawDataFileNames) {
                const std::string kRawDataTerrablobPathStr = fmt::format("{}/{}", kTerrablobPathStrWithDate, raw_data_file_name);
                if (m_ingested_raw_data_terrablob_paths.end() == m_ingested_raw_data_terrablob_paths.find(kRawDataTerrablobPathStr)) {
                    while (false == download_raw_data_from_terrablob_(kRawDataTerrablobPathStr)) {
                        SPDLOG_ERROR("Failed to download, retrying it");
                    }
                    const std::filesystem::path kDownloadedRawDataPath{fmt::format("{}/{}", kDownloadedRawDataPathPrefix, raw_data_file_name)};
                    if (std::filesystem::exists(kDownloadedRawDataPath)) {
                        next_batch_paths.emplace_back(kDownloadedRawDataPath);
                        next_batch_of_current_region_cnt++;
                    }
                }
            }
            // Increase the current date to the next one, make sure it is not the last one
            const std::vector<std::string> kDates = get_tb_cli_ls_result_strings_(kTerrablobPathStrWithoutDate);
            // Iterate from the end, because the latest date will appear at the end, so that we can iterate less to find the next date
            for (size_t i{3}; i <= kDates.size(); ++i) {
                size_t id = kDates.size() - i;
                if (kCurrentDate == kDates[id]) {
                    region_date.second = parse_ptime_(kDates[id + 1]);
                    SPDLOG_INFO("{} {} next date is {}", zone.first, region_date.first, kDates[id + 1]);
                    break;
                }
            }
            SPDLOG_INFO("{} {} put {} raw data from date {} into next batch", zone.first, region_date.first, next_batch_of_current_region_cnt, kCurrentDate);
        }
    }
    return next_batch_paths;
}

bool SawmillQueryRawDataHandler::download_raw_data_from_terrablob_(
    std::string_view terrablob_path_str) {
    const size_t kLastSlash = terrablob_path_str.find_last_of('/');
    const std::string kRawDataFileName = (std::string::npos == kLastSlash) ? std::string(terrablob_path_str) : std::string(terrablob_path_str).substr(kLastSlash + 1);
    const std::string kDownloadedRawDataPathStr = fmt::format("{}/{}", kDownloadedRawDataPathPrefix, kRawDataFileName);
    return 0 == system(fmt::format("tb-cli get {} {}", terrablob_path_str, kDownloadedRawDataPathStr).c_str());
}

std::vector<std::string> SawmillQueryRawDataHandler::get_tb_cli_ls_result_strings_(std::string_view terrablob_path_str) {
    std::vector<std::string> output;
    std::string command{fmt::format("tb-cli ls {}", terrablob_path_str)};
    char buffer[256];

    // Open a pipe to run the command.
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }

    // Read each line of output.
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        std::string line(buffer);
        // Remove trailing newline if present.
        if (!line.empty() && line.back() == '\n') {
            line.pop_back();
        }
        output.push_back(line);
    }

    // Close the pipe.
    pclose(pipe);
    return output;
}

}

// NOLINTBEGIN(bugprone-exception-escape)
auto main(int argc, char const* argv[]) -> int {
    // Parse the storage backend URL from the command line arguments
    if (argc < 6) {
        std::cerr << "Usage: ./client <storage-backend-url> <topic> <input_file_type>(json/irv2) <timestamp_key> <buffer_size>(unit: bytes)" << '\n';
        return 1;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::string const storage_url{argv[1]};
    if (storage_url.empty()) {
        std::cerr << "storage-backend-url cannot be empty." << '\n';
        return 1;
    }
    // Create a driver that connects to the Spider cluster
    spider::Driver driver{storage_url};

    std::string const topic{argv[2]};
    clp_s::FileType input_file_type;
    if ("irv2" == argv[3]) {
        input_file_type = clp_s::FileType::KeyValueIr;
    } else {
        input_file_type = clp_s::FileType::Json;
    }
    std::string timestamp_key{argv[4]};
    size_t buffer_size = std::stoul(argv[5]);

    if ("sawmill-query" == topic) {
        auto handler = std::make_unique<terrablob::SawmillQueryRawDataHandler>();
        std::vector<spider::Job<int>> jobs;
        while (true) {
            jobs.clear();
            const std::vector<std::filesystem::path> kNextBathRawDataPaths = handler->get_next();
            for (auto const& raw_data_path : kNextBathRawDataPaths) {
                jobs.emplace_back(driver.start(&compress, std::string_view(topic), static_cast<uint8_t>(input_file_type), std::string_view(timestamp_key), buffer_size, std::string_view(raw_data_path.string())));
            }

            // Wait for the jobs to complete
            for (auto& job : jobs) {
                job.wait_complete();
                // Handle the job's success/failure
                switch (auto job_status = job.get_status()) {
                    case spider::JobStatus::Succeeded: {
                        break;
                    }
                    case spider::JobStatus::Failed: {
                        std::pair<std::string, std::string> const error_and_fn_name = job.get_error();
                        SPDLOG_ERROR("Job failed in function {}-{}", error_and_fn_name.second, error_and_fn_name.first);
                        // TODO: if failed, then append the failed files
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Job is in unexpected state - {}", static_cast<std::underlying_type_t<decltype(job_status)>>(job_status));
                        // TODO: if failed, then append the failed files
                        break;
                    }
                }
            }
        }
    } else {
        SPDLOG_ERROR("Not supported other topics yet");
        return 1;
    }
}

// NOLINTEND(bugprone-exception-escape)
