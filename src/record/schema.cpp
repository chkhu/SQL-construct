#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
    char *p = buf;
    uint32_t ofs = GetSerializedSize();
    ASSERT(ofs <= PAGE_SIZE, "Failed to serialize schema.");
    // magic num
    MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
    buf += 4;
    // table nums
    MACH_WRITE_UINT32(buf, columns_.size());
    buf += 4;
    // table schema
    for (auto column: columns_) {
        buf += column->SerializeTo(buf);
    }
    return buf - p;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t size = 4 + 4;
    for (auto column: columns_) {
        size += column->GetSerializedSize();
    }
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    if (schema != nullptr) {
        LOG(WARNING) << "Pointer object schema is not null in schema deserialize." << std::endl;
    }
    char *p = buf;
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Failed to deserialize schema.");
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    std::vector<Column *> columns;
    for (uint32_t i = 0; i < table_nums; i++) {
        Column *column = nullptr;
        buf += Column::DeserializeFrom(buf, column);
        columns.emplace_back(column);
    }
    bool is_manage = MACH_READ_UINT32(buf) != 0;
    schema = new Schema(columns, is_manage);
    return buf - p;
}

uint32_t Schema::GetColId(std::string column_name) const {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i]->GetName() == column_name) {
            return i;
        }
    }
}
