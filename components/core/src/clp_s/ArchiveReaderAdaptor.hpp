#ifndef CLP_S_ARCHIVEREADERADAPTOR_HPP
#define CLP_S_ARCHIVEREADERADAPTOR_HPP

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "../clp/CheckpointReader.hpp"
#include "../clp/ReaderInterface.hpp"
#include "SingleFileArchiveDefs.hpp"
#include "TimestampDictionaryReader.hpp"
#include "TraceableException.hpp"
#include "ZstdDecompressor.hpp"

namespace clp_s {
class ArchiveReaderAdaptor {
public:
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    explicit ArchiveReaderAdaptor(std::string path, bool single_file_archive);

    ~ArchiveReaderAdaptor();

    ErrorCode load_archive_metadata();

    clp::ReaderInterface& checkout_reader_for_section(std::string_view section);

    void checkin_reader_for_section(std::string_view section);

    std::shared_ptr<TimestampDictionaryReader> get_timestamp_dictionary() {
        return m_timestamp_dictionary;
    }

private:
    ErrorCode try_read_archive_file_info(ZstdDecompressor& decompressor, size_t size);

    ErrorCode try_read_timestamp_dictionary(ZstdDecompressor& decompressor, size_t size);

    // TODO: change this to metadata option
    bool m_single_file_archive{false};
    std::string m_path;
    ArchiveFileInfoPacket m_archive_file_info{};
    ArchiveHeader m_archive_header{};
    size_t m_files_section_offset{};
    std::optional<std::string> m_current_reader_holder;
    std::shared_ptr<TimestampDictionaryReader> m_timestamp_dictionary;

    // TODO: switch to readerinterface
    std::shared_ptr<clp::ReaderInterface> m_reader;
    clp::CheckpointReader m_checkpoint_reader{nullptr, 0};
};

}  // namespace clp_s
#endif  // CLP_S_ARCHIVEREADERADAPTOR_HPP
