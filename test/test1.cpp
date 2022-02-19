#include "Mocks.hpp"
#include <search/DocSimple.hpp>

#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

struct TestSearch : public testing::Test {
  TestSearch() {
    m_WorkingDir = fs::current_path() / "test-data";
    if (fs::is_directory(m_WorkingDir)) {
      (void)fs::remove_all(m_WorkingDir);
    }
    fs::create_directories(m_WorkingDir);
  }

  ~TestSearch() {
    (void)fs::remove_all(m_WorkingDir);
    m_WorkingDir.clear();
  }

  const fs::path& path() const { return m_WorkingDir; }

private:
  fs::path m_WorkingDir;
};

struct DbSimpleTest : public TestSearch {
  typedef Db<FileStore<DocSimple>> TSearchDb;
  typedef std::vector<typename TSearchDb::TStore::TDoc::TId> TRes;

  FileStore<typename TSearchDb::TStore::TDoc> store;
  TSearchDb db;

  DbSimpleTest() : store(path() / "db"), db(store) {}

  std::vector<typename TSearchDb::TStore::TDoc::TId>
  search(std::string_view query) {
    SearchSettings<typename TSearchDb::TStore::TDoc> sett;
    sett.query = query;
    CompIsWhole<Result<typename TSearchDb::TStore::TDoc>> cmp1;
    CompWordsTogether<Result<typename TSearchDb::TStore::TDoc>> cmp2;
    auto result = findMany<TSearchDb>({&db}, sett, cmp1, cmp2);

    std::vector<typename TSearchDb::TStore::TDoc::TId> arr(result.size());
    for (size_t i = 0; i < result.size(); ++i) {
      arr[i] = result[i].id;
    }
    return arr;
  }
};

TEST_F(DbSimpleTest, Add1) {
  db.add(DocSimple(1, "abc def"));
  db.add(DocSimple(2, "ghi jkl"));
  EXPECT_EQ(search("abc"), TRes{1});
}

TEST_F(DbSimpleTest, Add2) {
  db.add(DocSimple(1, "abc def"));
  db.add(DocSimple(2, "ghi jkl"));
  EXPECT_EQ(search("ghi jkl"), TRes{2});
}

TEST_F(DbSimpleTest, Add3) {
  db.add(DocSimple(1, "abc def"));
  db.add(DocSimple(2, "ghi jkl"));
  EXPECT_EQ(search("abc ghi"), TRes{});
}
