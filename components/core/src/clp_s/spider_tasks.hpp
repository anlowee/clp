#ifndef TASKS_HPP
#define TASKS_HPP

#include <spider/client/spider.hpp>
#include <string>
#include <vector>

// Task function prototype
/**
 * @param context
 * @param s3_paths vector of s3 object URLs
 * @param destination
 * @return The sum of x and y.
 */
int
compress(spider::TaskContext& context, std::vector<std::string> s3_paths, std::string destination);

#endif  // TASKS_HPP
