#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());

  // Scenario: get three victims from the lru.
  int value;
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);
  lru_replacer.Pin(4);
  EXPECT_EQ(2, lru_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);

  // Scenario: continue looking for victims. We expect these victims.
  lru_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

// TEST(LRUReplacerTest, ClockTest){
//   ClockReplacer clock_replacer(10);
//   //add to clock replacer
//   clock_replacer.Unpin(1);
//   clock_replacer.Unpin(2);
//   clock_replacer.Unpin(3);
//   clock_replacer.Unpin(1);
//   clock_replacer.Unpin(4);
//   clock_replacer.Unpin(5);
//   clock_replacer.Unpin(1);
//   clock_replacer.Unpin(6);

//   EXPECT_EQ(6, clock_replacer.Size());

//   int value;
//   clock_replacer.Victim(&value);
//   EXPECT_EQ(6, value);
//   clock_replacer.Victim(&value);
//   EXPECT_EQ(4, clock_replacer.Size());
//   EXPECT_EQ(1, value);

//   //6 is not in the list, so nothing happened
//   clock_replacer.Pin(6);
//   clock_replacer.Pin(3);
//   EXPECT_EQ(3, clock_replacer.Size());
// } 

TEST(LRUReplacerTest, ClockTest){
  ClockReplacer clock_replacer(10);
  //add to clock replacer
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(1);

  EXPECT_EQ(6, clock_replacer.Size());

  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);

  clock_replacer.Unpin(2);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);
  EXPECT_EQ(4, clock_replacer.Size());

  clock_replacer.Pin(1);
  // 1 is not in the list, so nothing happened

  clock_replacer.Pin(4);
  EXPECT_EQ(3, clock_replacer.Size());
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  EXPECT_EQ(2, clock_replacer.Size());
} 