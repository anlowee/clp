#ifndef CLP_CHECKPOINTREADER_HPP
#define CLP_CHECKPOINTREADER_HPP

#include "ReaderInterface.hpp"

namespace clp {
class CheckpointReader : public ReaderInterface {
public:
    explicit CheckpointReader(ReaderInterface* reader, size_t checkpoint)
            : m_reader(reader),
              m_checkpoint(checkpoint) {
        if (nullptr != m_reader) {
            m_cur_pos = m_reader->get_pos();
        }
        if (m_cur_pos > m_checkpoint) {
            throw ReaderInterface::OperationFailed(ErrorCode_BadParam, __FILE__, __LINE__);
        }
    }

    ErrorCode try_get_pos(size_t& pos) override { return m_reader->try_get_pos(pos); }

    ErrorCode try_seek_from_begin(size_t pos) override;

    ErrorCode try_read(char* buf, size_t num_bytes_to_read, size_t& num_bytes_read) override;

    ErrorCode
    try_read_to_delimiter(char delim, bool keep_delimiter, bool append, std::string& str) override {
        // TODO: implement later if necessary
        return ErrorCode_Failure;
    }

private:
    ReaderInterface* m_reader{nullptr};
    size_t m_checkpoint{};
    size_t m_cur_pos{};
};
}  // namespace clp

#endif  // CLP_CHECKPOINTREADER_HPP
