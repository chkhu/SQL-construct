#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}


uint32_t Column::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize column data to disk.");
  char *p = buf;
  // Write the magic number to the buffer
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += sizeof(uint32_t);

  // Get the length of the column name and write it to the buffer
  uint32_t column_name_length = name_.length();
  MACH_WRITE_UINT32(buf, column_name_length);
  buf += sizeof(uint32_t);

  // Write the column name string to the buffer
  MACH_WRITE_STRING(buf, name_);
  buf += column_name_length;

  // Write the type, length, column index, nullable flag, and unique flag to the buffer
  MACH_WRITE_UINT32(buf, static_cast<uint32_t>(type_));
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, len_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, table_ind_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, nullable_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, unique_);
  buf += sizeof(uint32_t);

  // Return the number of bytes written to the buffer
  return buf - p;
}

uint32_t Column::GetSerializedSize() const {
  // Return the size of the column object when serialized
  return sizeof(uint32_t) * 7 + name_.length();
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }

  /* deserialize field from buf */
  // Allocate memory for the column object
  void *mem = malloc(sizeof(Column));
  char *p = buf;

  // Read the magic number from buf
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Failed to deserialize column info.");

  // Read the length of the column name from buf
  uint32_t column_name_length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // Read the column name string from buf
  std::string column_name = MACH_READ_STRING(buf, column_name_length);
  buf += column_name_length;

  // Read the type, length, column index, nullable flag, and unique flag from buf
  TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buf));
  buf += sizeof(uint32_t);
  uint32_t length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t col_ind = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  bool nullable = MACH_READ_UINT32(buf) != 0;
  buf += sizeof(uint32_t);
  bool unique = MACH_READ_UINT32(buf) != 0;
  buf += sizeof(uint32_t);

  // Create a new Column object based on the deserialized information
  if (type == TypeId::kTypeChar) {
    column = new (mem) Column(column_name, type, length, col_ind, nullable, unique);
  } else {
    column = new (mem) Column(column_name, type, col_ind, nullable, unique);
  }

  // Return the number of bytes read from buf
  return buf - p;
}
