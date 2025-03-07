#ifndef TASKS_HPP
#define TASKS_HPP

#include "InputConfig.hpp"

#include <spider/client/spider.hpp>
#include <filesystem>

// Task function prototype
/**
 * @param context
 * @param topic The name of the topic (table name)
 * @param input_file_type_uint8 Json or IRV2
 * @param timestamp_key The timestamp_key flag used by clp-s
 * @param buffer_size When the raw data accumulates more than the buffer_size the compression starts
 * @param raw_data_path_str The path of downloaded raw data file path
 * @return 0-Success, 1-Waiting for more data, 2-Exception, other-failed
 */
int compress(
        spider::TaskContext& context,
        std::string_view topic,
        uint8_t input_file_type_uint8,
        std::string_view timestamp_key,
        size_t buffer_size,
        std::string_view raw_data_path_str
);

#endif  // TASKS_HPP
