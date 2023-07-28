#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"


char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0"), const_cast<char *>("12234a547854562131")};
Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),  Field(TypeId::kTypeInt, 1078),
        Field(TypeId::kTypeInt, -4589),   Field(TypeId::kTypeInt, 187)      };
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
    Field(TypeId::kTypeFloat,0.121385f),
    Field(TypeId::kTypeFloat,157.89f)
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false),
                       Field(TypeId::kTypeChar,chars[4], strlen(chars[4]),true)
};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};



TEST(TupleTest, SerializeDeserializeTest) {
  vector<Field> row_int_field_0{Field(int_fields[0]),Field(char_fields[0]),Field(float_fields[0])};
  vector<Field> row_int_field_1{Field(int_fields[1]),Field(char_fields[1]),Field(float_fields[1])};
  vector<Field> row_int_field_2{Field(int_fields[1]),Field(char_fields[2]),Field(float_fields[2])};
  vector<Field> row_int_field_3{Field(int_fields[1]),Field(char_fields[3]),Field(float_fields[3])};

  Column columns_[] = {
      Column("ID",TypeId::kTypeInt,0,false, true),
      Column("Name",TypeId::kTypeChar,5,1,false,true),
      Column("score", TypeId::kTypeFloat,2,false,false)
  };
  vector<Column*> collumn_in_schema{&columns_[0],&columns_[1],&columns_[2]};
  Schema* schema_ = new Schema(collumn_in_schema);
  Row rows_[] = {
      Row(row_int_field_0),Row(row_int_field_1),Row(row_int_field_2),Row(row_int_field_3)
  };

  char buffer[PAGE_SIZE];
  char* p0 = buffer;
  memset(buffer, 0, sizeof(buffer));
  for(int i = 0;i < 3; i++){
    p0 += columns_[i].SerializeTo(p0);
  }
  p0 += schema_->SerializeTo(p0);
  for(int i = 0; i < 4; i++){
    p0 += rows_[i].SerializeTo(p0,schema_);
  }
  uint32_t ofs0 = 0;
  Column *c0 = nullptr;
  Schema* s0 = nullptr;
  Row* r0 = nullptr;
  for (int i = 0; i < 3; i++) {
    ofs0 += Column::DeserializeFrom(buffer + ofs0, c0);
    EXPECT_EQ(c0->GetName(), columns_[i].GetName());
    EXPECT_EQ(c0->GetType(), columns_[i].GetType());
    EXPECT_EQ(c0->IsUnique(), columns_[i].IsUnique());
    delete c0;
    c0 = nullptr;
  }
  ofs0 += Schema::DeserializeFrom(buffer+ofs0, s0);
  EXPECT_EQ(s0->GetColumnCount(),schema_->GetColumnCount());
  delete s0;
  s0 = nullptr;
  for (int i = 0; i < 4; i++) {
    r0 = new Row;
    ofs0 += r0->DeserializeFrom(buffer + ofs0, schema_);
    ASSERT_EQ(r0->GetFieldCount(),rows_[i].GetFieldCount());
    ASSERT_EQ(r0->GetField(0)->GetTypeId(),rows_[i].GetField(0)->GetTypeId());
    delete r0;
    r0 = nullptr;
  }
  // Serialize phase
  char *p = buffer;
  for (int i = 0; i < 8; i++) {
    if(i==4) continue;
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 6; i++) {
    if(i==3) continue;
    p += float_fields[i].SerializeTo(p);
  }
  for (const auto &char_field : char_fields) {
    p += char_field.SerializeTo(p);
  }

  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 8; i++) {
    if(i==4) continue;
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 6; i++) {
    if(i==3) continue;
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    delete df;
    df = nullptr;
  }
}

TEST(TupleTest, RowTest) {
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  RowId new_id, old_id = first_tuple_rid;
  for(int i = 0; i < 100; i++){
    Row new_row(fields);
    table_page.InsertTuple(new_row,schema.get(), nullptr, nullptr, nullptr);
    ASSERT_TRUE(table_page.GetNextTupleRid(old_id,&new_id));
    ASSERT_EQ(new_row.GetRowId(),new_id);
    Row old_row(old_id);
    ASSERT_TRUE(table_page.GetTuple(&old_row,schema.get(), nullptr, nullptr));
    std::vector<Field *> &old_row_fields = old_row.GetFields();
    ASSERT_EQ(3, old_row_fields.size());
    for (size_t i = 0; i < old_row_fields.size(); i++) {
      ASSERT_EQ(CmpBool::kTrue, old_row_fields[i]->CompareEquals(fields[i]));
    }
    old_id = new_id;
  }
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}