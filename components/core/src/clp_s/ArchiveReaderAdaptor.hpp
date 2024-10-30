#ifndef CLP_S_ARCHIVEREADERADAPTOR_HPP
#define CLP_S_ARCHIVEREADERADAPTOR_HPP

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "../clp/CheckpointReader.hpp"
#include "../clp/ReaderInterface.hpp"
#include "Defs.hpp"
#include "SingleFileArchiveDefs.hpp"
#include "TimestampDictionaryReader.hpp"
#include "TraceableException.hpp"
#include "ZstdDecompressor.hpp"

namespace clp_s {
/**
 * ArchiveReaderAdaptor is an adaptor class which helps with reading single and multi-file archives
 * which exist on either S3 or a locally mounted file system.
 */
class ArchiveReaderAdaptor {
public:
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    explicit ArchiveReaderAdaptor(
            std::string path,
            InputOption const& input_config,
            bool single_file_archive
    );

    ~ArchiveReaderAdaptor();

    /**
     * Load metadata for an archive including the header and metadata section. This method must be
     * invoked before checking out any section of an archive, or calling `get_timestamp_dictionary`.
     * @return ErrorCodeSuccess on success
     * @return ErrorCode_errno on failure
     */
    ErrorCode load_archive_metadata();

    /**
     * Checkout a reader for a given section of the archive. Reader must be checked back in with the
     * `checkin_reader_for_section` method.
     * @param section
     * @return A ReaderInterface opened and pointing to the requested section.
     * @throw OperationFailed if a reader is already checked out, or checking out this section would
     *        force a backwards seek.
     */
    clp::ReaderInterface& checkout_reader_for_section(std::string_view section);

    /**
     * Checkin a reader for a given section of the archive.
     * @param section
     * @throw OperationFailed if no reader is checked out, or if the section being checked in does
     *        not match the section currently checked out.
     */
    void checkin_reader_for_section(std::string_view section);

    std::shared_ptr<TimestampDictionaryReader> get_timestamp_dictionary() {
        return m_timestamp_dictionary;
    }

    ArchiveHeader const& get_header() const { return m_archive_header; }

private:
    ErrorCode try_read_archive_file_info(ZstdDecompressor& decompressor, size_t size);

    ErrorCode try_read_timestamp_dictionary(ZstdDecompressor& decompressor, size_t size);

    bool m_single_file_archive{false};
    std::string m_path;
    ArchiveFileInfoPacket m_archive_file_info{};
    ArchiveHeader m_archive_header{};
    size_t m_files_section_offset{};
    std::optional<std::string> m_current_reader_holder;
    std::shared_ptr<TimestampDictionaryReader> m_timestamp_dictionary;

    std::shared_ptr<clp::ReaderInterface> m_reader;
    clp::CheckpointReader m_checkpoint_reader{nullptr, 0};
    InputOption m_input_config{};
};

}  // namespace clp_s
#endif  // CLP_S_ARCHIVEREADERADAPTOR_HPP
