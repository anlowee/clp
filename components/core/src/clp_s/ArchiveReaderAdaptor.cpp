#include "ArchiveReaderAdaptor.hpp"

#include <string>
#include <string_view>
#include <vector>

#include <msgpack.hpp>
#include <spdlog/spdlog.h>

#include "../clp/CheckpointReader.hpp"
#include "archive_constants.hpp"
#include "Defs.hpp"
#include "ReaderUtils.hpp"

namespace clp_s {

ArchiveReaderAdaptor::ArchiveReaderAdaptor(
        std::string path,
        InputOption const& input_config,
        bool single_file_archive
)
        : m_path(path),
          m_single_file_archive(single_file_archive),
          m_timestamp_dictionary(std::make_shared<TimestampDictionaryReader>()),
          m_input_config{input_config} {
    if (false == m_single_file_archive) {
        // TODO: support both
        throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
    }
}

ArchiveReaderAdaptor::~ArchiveReaderAdaptor() {
    m_reader.reset();
}

ErrorCode
ArchiveReaderAdaptor::try_read_archive_file_info(ZstdDecompressor& decompressor, size_t size) {
    std::vector<char> buffer;
    buffer.resize(size);
    auto rc = decompressor.try_read_exact_length(buffer.data(), size);
    if (ErrorCodeSuccess != rc) {
        return rc;
    }

    try {
        auto obj_handle = msgpack::unpack(buffer.data(), buffer.size());
        auto obj = obj_handle.get();
        // m_archive_file_info = obj.as<clp_s::ArchiveFileInfoPacket>();
        //  FIXME: the above should work, but does not. Hacking around it as below for now.
        if (obj.is_nil() || msgpack::type::MAP != obj.type) {
            return ErrorCodeCorrupt;
        }
        if (nullptr == obj.via.map.ptr) {
            return ErrorCodeCorrupt;
        }
        auto val = obj.via.map.ptr->val;
        if (val.is_nil() || msgpack::type::ARRAY != val.type) {
            return ErrorCodeCorrupt;
        }
        if (nullptr == val.via.array.ptr) {
            return ErrorCodeCorrupt;
        }
        auto arr = val.via.array;
        for (size_t i = 0; i < arr.size; ++i) {
            auto array_element = arr.ptr[i].as<clp_s::ArchiveFileInfo>();
            m_archive_file_info.files.push_back(array_element);
        }
        return ErrorCodeSuccess;
    } catch (std::exception const& e) {
        return ErrorCodeCorrupt;
    }
}

ErrorCode
ArchiveReaderAdaptor::try_read_timestamp_dictionary(ZstdDecompressor& decompressor, size_t size) {
    return m_timestamp_dictionary->read(decompressor);
}

ErrorCode ArchiveReaderAdaptor::load_archive_metadata() {
    constexpr size_t cDecompressorFileReadBufferCapacity = 64 * 1024;
    std::string path = m_path;
    if (InputSource::Filesystem == m_input_config.source) {
        path += clp_s::constants::cArchiveFile;
    }
    m_reader = ReaderUtils::try_create_reader(path, m_input_config);
    if (nullptr == m_reader) {
        return ErrorCodeFileNotFound;
    }

    std::array<char, sizeof(ArchiveHeader)> header_buffer;
    auto clp_rc = m_reader->try_read_exact_length(
            reinterpret_cast<char*>(&m_archive_header),
            sizeof(m_archive_header)
    );
    if (clp::ErrorCode::ErrorCode_Success != clp_rc) {
        return ErrorCodeErrno;
    }

    // TODO validate magic number, version, compression type, etc.
    m_files_section_offset = sizeof(m_archive_header) + m_archive_header.metadata_section_size;

    m_checkpoint_reader = clp::CheckpointReader{m_reader.get(), m_files_section_offset};

    ZstdDecompressor decompressor;
    decompressor.open(m_checkpoint_reader, cDecompressorFileReadBufferCapacity);

    uint8_t num_metadata_packets{};
    auto rc = decompressor.try_read_numeric_value(num_metadata_packets);
    if (ErrorCodeSuccess != rc) {
        return rc;
    }

    for (size_t i = 0; i < num_metadata_packets; ++i) {
        ArchiveMetadataPacketType packet_type;
        uint32_t packet_size;
        rc = decompressor.try_read_numeric_value(packet_type);
        if (ErrorCodeSuccess != rc) {
            return rc;
        }
        rc = decompressor.try_read_numeric_value(packet_size);
        if (ErrorCodeSuccess != rc) {
            return rc;
        }

        switch (packet_type) {
            case ArchiveMetadataPacketType::ArchiveFileInfo:
                rc = try_read_archive_file_info(decompressor, packet_size);
                break;
            case ArchiveMetadataPacketType::TimestampDictionary:
                rc = try_read_timestamp_dictionary(decompressor, packet_size);
                break;
            case ArchiveMetadataPacketType::ArchiveInfo: {
                std::vector<char> buf;
                buf.resize(packet_size);
                rc = decompressor.try_read_exact_length(buf.data(), buf.size());
            }
            default:
                break;
        }
        if (ErrorCodeSuccess != rc) {
            return rc;
        }
    }

    decompressor.close();
    return ErrorCodeSuccess;
}

clp::ReaderInterface& ArchiveReaderAdaptor::checkout_reader_for_section(std::string_view section) {
    if (m_current_reader_holder.has_value()) {
        throw OperationFailed(ErrorCodeNotReady, __FILENAME__, __LINE__);
    }

    auto it = std::find_if(
            m_archive_file_info.files.begin(),
            m_archive_file_info.files.end(),
            [&](ArchiveFileInfo& info) { return info.n == section; }
    );
    if (m_archive_file_info.files.end() == it) {
        throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
    }

    size_t cur_pos{};
    if (auto rc = m_reader->try_get_pos(cur_pos); clp::ErrorCode::ErrorCode_Success != rc) {
        throw OperationFailed(ErrorCodeFailure, __FILENAME__, __LINE__);
    }

    size_t file_offset = m_files_section_offset + it->o;
    ++it;
    size_t next_file_offset{m_archive_header.compressed_size};
    if (m_archive_file_info.files.end() != it) {
        next_file_offset = m_files_section_offset + it->o;
    }

    if (cur_pos > file_offset) {
        throw OperationFailed(ErrorCodeCorrupt, __FILENAME__, __LINE__);
    }

    if (cur_pos != file_offset) {
        if (auto rc = m_reader->try_seek_from_begin(file_offset);
            clp::ErrorCode::ErrorCode_Success != rc)
        {
            throw OperationFailed(ErrorCodeFailure, __FILENAME__, __LINE__);
        }
    }

    m_current_reader_holder.emplace(section);
    m_checkpoint_reader = clp::CheckpointReader{m_reader.get(), next_file_offset};
    return m_checkpoint_reader;
}

void ArchiveReaderAdaptor::checkin_reader_for_section(std::string_view section) {
    if (false == m_current_reader_holder.has_value()) {
        throw OperationFailed(ErrorCodeNotInit, __FILENAME__, __LINE__);
    }

    if (m_current_reader_holder.value() != section) {
        throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
    }

    m_current_reader_holder.reset();
}

}  // namespace clp_s
