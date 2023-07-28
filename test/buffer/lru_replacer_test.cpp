#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(10000);

  // Scenario: unpin 99 elements, i.e. add them to the replacer.
  for(int i = 1; i < 10000; i++){
    lru_replacer.Unpin(i);
  }
//  lru_replacer.Unpin(1);
//  lru_replacer.Unpin(2);
//  lru_replacer.Unpin(3);
//  lru_replacer.Unpin(4);
//  lru_replacer.Unpin(5);
//  lru_replacer.Unpin(6);
//  lru_replacer.Unpin(1);
  for(int i = 1; i < 5000;i++){
    lru_replacer.Unpin(i);
  }
  EXPECT_EQ(9999, lru_replacer.Size());
  EXPECT_EQ(9999,lru_replacer.UnpinSize());
  // Scenario: get 59 victims from the lru.
  int value;
  for(int i = 1; i < 4000; i++){
    lru_replacer.Victim(&value);
    EXPECT_EQ(i, value);
    EXPECT_EQ(9999-i, lru_replacer.Size());
    EXPECT_EQ(9999-i,lru_replacer.UnpinSize());
    EXPECT_EQ(0,lru_replacer.PinSize());
  }
  // Scenario: pin elements in the replacer.
  for(int i = 0; i < 1000; i++){
    lru_replacer.Pin(i+4000);
    EXPECT_EQ(6000, lru_replacer.Size());
    EXPECT_EQ(5999-i,lru_replacer.UnpinSize());
    EXPECT_EQ(i+1,lru_replacer.PinSize());
  }

  // Scenario: unpin 60、61、62、63. We expect that the reference them be set to first.
  lru_replacer.Unpin(4800);
  lru_replacer.Unpin(4801);
  lru_replacer.Unpin(4802);
  lru_replacer.Unpin(4803);

  // Scenario: continue looking for victims. We expect these victims.
  for(int i = 5000; i < 10000; i++){
    lru_replacer.Victim(&value);
    EXPECT_EQ(i, value);
  }

  lru_replacer.Victim(&value);
  EXPECT_EQ(4800, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4801, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4802, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4803, value);
  EXPECT_EQ(false, lru_replacer.Victim(&value));
  EXPECT_EQ(996,lru_replacer.Size());
}