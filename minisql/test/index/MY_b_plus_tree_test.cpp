#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, DISABLED_SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 3, 4);
  // size 3,4 to be the same as https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html

  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 500; // bigger
  // const int n = 30; 
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }

  //Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  
  // keys = {6 ,8 ,9 ,0 ,14,4 ,13,1 ,3 ,10,11,12,5 ,7 ,2 };
  // values={14,10, 8, 0,13, 6, 9, 3, 4, 7,12,11, 2, 1, 5};
  // delete_seq={14,6,12,11,7,9,10,3,5,2,0,1,13,4,8};

  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    LOG(INFO)<<"Insert "<<keys[i]<<" "<<values[i];
    tree.Insert(keys[i], values[i]);
    tree.PrintTree(mgr[i+1000]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys[i], ans);
    LOG(INFO)<< "size after "<<i<<" is "<<ans.size();
    LOG(INFO) << "key: " << keys[i] << ", kv_map[i]: " << kv_map[keys[i]] << ", ans[i]: " << ans[i] << endl;
    ASSERT_EQ(kv_map[keys[i]], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    LOG(INFO)<<"Remove key "<<delete_seq[i]<<" "<<values[i];
    tree.Remove(delete_seq[i]);
    tree.PrintTree(mgr[i+2000]);
  }
  tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    LOG(INFO) << "delete_seq[i]: " << delete_seq[i];
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }

  // clear all tree file
  // for (size_t i = 0; i < n; i++)
  // {
  //   string file_name = "tree_"+to_string(i+1000)+".txt";
  //   remove(file_name.c_str());
  // }
}