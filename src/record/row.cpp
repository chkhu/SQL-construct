#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    char *p = buf;
    RowId rid = GetRowId();
    MACH_WRITE_UINT32(buf, rid.GetPageId());
    buf += 4;
    MACH_WRITE_UINT32(buf, rid.GetSlotNum());
    buf += 4;
    std::vector<bool> nulls;
    for (auto field: fields_) {
        nulls.push_back(field->IsNull());
    }
    for (auto null: nulls) {
        MACH_WRITE_UINT8(buf, null);
        buf += 1;
    }
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        if (nulls[i]) {
            continue;
        }
        buf += fields_[i]->SerializeTo(buf);
    }
    return buf - p;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");
    char *p = buf;
    uint32_t page_id = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t slot_num = MACH_READ_UINT32(buf);
    buf += 4;
    rid_ = RowId(page_id, slot_num);
    std::vector<bool> nulls;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        nulls.push_back(MACH_READ_UINT8(buf) != 0);
        buf += 1;
    }
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        Field *field = nullptr;
        buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &field, nulls[i]);
        fields_.emplace_back(field);
    }
    return buf - p;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    uint32_t size = 4 + 4;
    size += fields_.size();
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        if (fields_[i]->IsNull()) {
            continue;
        }
        size += fields_[i]->GetSerializedSize();
    }
    return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
    auto columns = key_schema->GetColumns();
    std::vector<Field> fields;
    uint32_t idx;
    for (auto column: columns) {
        schema->GetColumnIndex(column->GetName(), idx);
        fields.emplace_back(*this->GetField(idx));
    }
    key_row = Row(fields);
}

bool Row::operator<(const Row &other) const {
    uint32_t column_count = fields_.size();
    for (uint32_t i = 0; i < column_count; i++) {
        Field *lhs_value = this->GetField(i);
        Field *rhs_value = other.GetField(i);

        if (lhs_value->CompareLessThan(*rhs_value) == CmpBool::kTrue) {
            return true;
        }

        if (lhs_value->CompareGreaterThan(*rhs_value) == CmpBool::kTrue) {
            return false;
        }
    }
    return false;
}