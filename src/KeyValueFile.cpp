#include <search/KeyValueFile.hpp>

#include <boost/endian/conversion.hpp>
#include <city.h>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>

// for ram
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#define makeStringView(str) BytesView(str.data(), str.size())

namespace Search {

const uint64_t KeyValueFile::Version;

KeyValueFile::KeyValueFile(const fs::path& path)
    : path_(path), locked_(false), buffer_(nullptr), importing_(nullptr) {
  if (!path.empty()) {
    if (!fs::is_regular_file(path_)) {
      // create empty file
      createFile(path);
    }
    openFile();
  }
}

KeyValueFile::~KeyValueFile() {
  if (importing_) {
    delete importing_;
    importing_ = nullptr;
  }
}

void KeyValueFile::createFile(const fs::path& path, uint64_t tabSize,
                              uint64_t contentSize) {
  // create
  { std::ofstream output(path.string()); }
  fs::resize_file(path, 100 + tabSize * sizeof(uint64_t) + contentSize);

  boost::iostreams::mapped_file file;
  file.open(path);
  if (!file.is_open()) {
    throw std::runtime_error("Cant open file2");
  }
  assert(file.data());

  uint64_t numWasted = 0;
  uint64_t nextDataOffset2 = 100 + tabSize * sizeof(uint64_t);
  uint64_t numItems = 0;

  /*
  - - - - - - - - - - -
  Header structure:
  - - - - - - - - - - -
   * version
   * number of slots in hash table
   * number of wasted bytes
   * next data offset
   * num items
  - - - - - - - - - - -
  */

  const auto v = boost::endian::native_to_little<uint64_t>(Version);
  std::memcpy(file.data() + 0 * sizeof(uint64_t), &v, sizeof(uint64_t));
  std::memcpy(file.data() + 1 * sizeof(uint64_t), &tabSize, sizeof(uint64_t));
  std::memcpy(file.data() + 2 * sizeof(uint64_t), &numWasted, sizeof(uint64_t));
  std::memcpy(file.data() + 3 * sizeof(uint64_t), &nextDataOffset2,
              sizeof(uint64_t));
  std::memcpy(file.data() + 4 * sizeof(uint64_t), &numItems, sizeof(uint64_t));

  file.close();
}

size_t KeyValueFile::fileSize() const { return file_.size(); }

void KeyValueFile::set(const std::string& key, const Bytes& value) {
  set(std::string_view(key), makeStringView(value));
}

void KeyValueFile::set(std::string_view key, BytesView value) {
  ensureTableSize(1);
  auto bucket = calcBucket(key);
  auto itemSize = Item::calcSize(key, value);
  ensureFreeSpace(itemSize);
  setInternal(bucket, key, value);
  ensureOptimalWaste();
}

void KeyValueFile::set(const KeyValueFile& db2) {
  ensureTableSize(db2.numItems());

  auto num1 = numBuckets();
  auto num2 = db2.numBuckets();
  for (uint64_t i = 0; i < num2; ++i) {
    auto it = db2.firstItem(i);
    while (it.valid()) {
      auto itemSize = Item::calcSize(it.key(), it.value());
      ensureFreeSpace(itemSize);

      uint64_t bucket;
      if (num1 == num2) {
        bucket = i;
      } else {
        bucket = calcBucket(it.key());
      }
      setInternal(bucket, it.key(), it.value());
      it = it.next();
    }
  }

  ensureOptimalWaste();
}

BytesView KeyValueFile::get(std::string_view key) const {
  return getWithBucket(calcBucket(key), key);
}

BytesView KeyValueFile::getWithBucket(uint64_t bucket,
                                      std::string_view key) const {
  auto pair = findInternal(bucket, key);
  if (pair.second.valid()) {
    return pair.second.value();
  }
  return {};
}

BytesView KeyValueFile::get(const std::string& key2) const {
  auto key = std::string_view(key2);
  return get(key);
}

void KeyValueFile::remove(std::string_view key) {
  auto bucket = calcBucket(key);
  removeInternal(bucket, key);
  ensureOptimalWaste();
}

void KeyValueFile::remove(const std::string& key2) {
  auto key = std::string_view(key2);
  return remove(key);
}

void KeyValueFile::setInternal(uint64_t bucket, std::string_view key,
                               BytesView value) {
  auto pair = findInternal(bucket, key);

  uint64_t prevOffset, nextOffset;
  if (pair.second.valid()) {
    // exists
    auto currentSize = pair.second.value().size();
    if (value.size() <= currentSize) {
      auto wasted2 = currentSize - value.size();
      pair.second.setValue(value);
      if (wasted2 != 0) {
        setWasted(wasted() + wasted2);
      }
      return;
    }
    setWasted(wasted() + currentSize);
    prevOffset = pair.first;
    nextOffset = pair.second.nextOffset();
  } else {
    prevOffset = 0;
    nextOffset = tableOffset(bucket);
    // change numitems
    setNumItems(numItems() + 1);
  }

  // add
  auto myOffset = nextDataOffset();
  auto dt = data() + myOffset;
  Item::write(dt, nextOffset, key, value);
  setNextDataOffset(dt - data());

  // link
  if (prevOffset == 0) {
    // write to table
    setTableOffset(bucket, myOffset);
  } else {
    // write to item
    Item itPrev(data(), prevOffset);
    itPrev.setNextOffset(myOffset);
  }
}

void KeyValueFile::removeInternal(uint64_t bucket, std::string_view key) {
  auto pair = findInternal(bucket, key);
  if (!pair.second.valid()) {
    return;
  }

  setNumItems(numItems() - 1);
  setWasted(wasted() + pair.second.calcSize());

  if (pair.first == 0) {
    // table
    setTableOffset(bucket, pair.second.nextOffset());
  } else {
    // item
    Item itPrev(data(), pair.first);
    itPrev.setNextOffset(pair.second.nextOffset());
  }
}

KeyValueFile::Item KeyValueFile::firstItem(uint64_t bucket) const {
  auto offset = tableOffset(bucket);
  return Item(data(), offset);
}

std::pair<uint64_t, KeyValueFile::Item>
KeyValueFile::findInternal(uint64_t bucket, std::string_view key) const {
  auto it = firstItem(bucket);
  uint64_t prevOffset = 0;
  while (it.valid()) {
    if (it.key() == key) {
      return {prevOffset, it};
    }
    prevOffset = it.offset();
    it = it.next();
  }
  return {0, KeyValueFile::Item()};
}

void KeyValueFile::bulkWrite(std::ofstream& out, std::string_view key,
                             BytesView value) {
  uint64_t hash = calcHash(key);
  out.write((const char*)&hash, sizeof(hash));

  auto len = Item::calcSize(key, value);
  auto len2 = writeSizeString(len);
  out.write((const char*)len2.data(), len2.size());

  Bytes buff(len, (std::byte)'\0');
  std::byte* buff2 = &buff[0];
  Item::write(buff2, 0, key, value);
  out.write((const char*)buff.data(), buff.size());
}

std::tuple<uint64_t, std::string_view, BytesView>
KeyValueFile::bulkRead(const std::byte*& dt) {
  uint64_t hash;
  std::memcpy(&hash, dt, sizeof(hash));
  dt += sizeof(hash);

  auto len = readSize(dt);
  std::byte* dt2 = (std::byte*)dt;
  Item it(dt2, 0);
  dt += len;
  return {hash, it.key(), it.value()};
}

void KeyValueFile::bulkStart(size_t numThreads) {
  assert(!importing_);
  std::unique_ptr<ImportingData> dt(new ImportingData());
  dt->numItems = numItems();
  dt->wasted = wasted();
  dt->dataRange.resize(numThreads, {0, 0});
  importing_ = dt.release();
}
void KeyValueFile::bulkStop() {
  assert(importing_);

  for (auto& pair : importing_->dataRange) {
    auto diff = pair.second - pair.first;
    importing_->wasted += diff;
  }

  std::unique_ptr<ImportingData> dt(importing_);
  importing_ = nullptr;
  setNumItems(dt->numItems);
  setWasted(dt->wasted);
}

void KeyValueFile::bulkInsertEnlarge(size_t nthThread, size_t numThreads) {
  // must be locked !!

  auto diff = importing_->dataRange[nthThread].second -
              importing_->dataRange[nthThread].first;
  importing_->wasted += diff;
  importing_->dataRange[nthThread].first = 0;
  importing_->dataRange[nthThread].second = 0;

  setWasted(importing_->wasted);
  setNumItems(importing_->numItems);
  if (wasted() > 100'000'000) {
    for (auto& pair : importing_->dataRange) {
      auto diff = pair.second - pair.first;
      if (diff > 0) {
        setWasted(wasted() + diff);
      }
      pair.first = 0;
      pair.second = 0;
    }

    uint64_t contentSize =
        nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
    changeTable(numBuckets(), contentSize);
    importing_->wasted = wasted();
    assert(importing_->numItems == numItems());
  }

  // ensure free space
  size_t reserve = 1'000'000;
  ensureFreeSpace(reserve);
  importing_->dataRange[nthThread].first = nextDataOffset();
  importing_->dataRange[nthThread].second =
      importing_->dataRange[nthThread].first + reserve;
  setNextDataOffset(importing_->dataRange[nthThread].second);
}

void KeyValueFile::bulkInsert(uint64_t bucket, std::string_view key,
                              BytesView value, size_t nthThread,
                              size_t numThreads) {
  auto itemSize = Item::calcSize(key, value);

  std::lock_guard lock1(importing_->mutex_);
  for (;;) {
    if (itemSize > importing_->dataRange[nthThread].second -
                       importing_->dataRange[nthThread].first) {
      bulkInsertEnlarge(nthThread, numThreads);
    } else {
      break;
    }
  }

  auto pair = findInternal(bucket, key);
  uint64_t prevOffset, nextOffset;
  if (pair.second.valid()) {
    // exists
    auto currentSize = pair.second.value().size();
    if (value.size() <= currentSize) {
      auto wasted2 = currentSize - value.size();
      pair.second.setValue(value);
      if (wasted2 != 0) {
        importing_->wasted += wasted2;
      }
      return;
    }
    importing_->wasted += currentSize;
    prevOffset = pair.first;
    nextOffset = pair.second.nextOffset();
  } else {
    prevOffset = 0;
    nextOffset = tableOffset(bucket);
    // change numitems
    importing_->numItems++;
  }

  // add
  auto myOffset = importing_->dataRange[nthThread].first;
  std::byte* dt3 = data() + myOffset;
  Item::write(dt3, nextOffset, key, value);
  importing_->dataRange[nthThread].first += itemSize;

  // link
  if (prevOffset == 0) {
    // write to table
    setTableOffset(bucket, myOffset);
  } else {
    // write to item
    Item itPrev(data(), prevOffset);
    itPrev.setNextOffset(myOffset);
  }
}

void KeyValueFile::bulkRemove(uint64_t bucket, std::string_view key,
                              BytesView value, size_t nthThread,
                              size_t numThreads) {
  std::lock_guard lock1(importing_->mutex_);

  auto pair = findInternal(bucket, key);
  if (!pair.second.valid()) {
    return;
  }

  importing_->numItems--;
  importing_->wasted += pair.second.calcSize();

  if (pair.first == 0) {
    // table
    setTableOffset(bucket, pair.second.nextOffset());
  } else {
    // item
    Item itPrev(data(), pair.first);
    itPrev.setNextOffset(pair.second.nextOffset());
  }
}

bool KeyValueFile::bulkIsInThread(uint64_t bucket, size_t nthThread,
                                  size_t numThreads, uint64_t numBuckets2) {
  if (numThreads == 1) {
    return true;
  }
  size_t perThread = numBuckets2 / numThreads;
  size_t start = nthThread * perThread;
  if (bucket < start) {
    return false;
  }
  if (bucket < start + perThread) {
    return true;
  }
  if (nthThread + 1 == numThreads) {
    return true;
  }
  return false;
}

uint64_t KeyValueFile::calcHash(std::string_view key) {
  return CityHash64((const char*)key.data(), key.size());
}

uint64_t KeyValueFile::calcBucketFromHash(uint64_t hash, uint64_t numBuckets2) {
  return hash % numBuckets2;
}

uint64_t KeyValueFile::calcBucket(std::string_view key) const {
  return calcBucketFromHash(calcHash(key), numBuckets());
}

std::byte* KeyValueFile::data() const {
  if (buffer_) {
    return buffer_;
  }
  std::byte* dt = (std::byte*)file_.data();
  return dt;
}

uint64_t KeyValueFile::tableOffset(uint64_t bucket) const {
  std::byte* dt = data();
  dt += 100;
  dt += bucket * sizeof(uint64_t);
  uint64_t n;
  std::memcpy(&n, dt, sizeof(n));
  return n;
}
void KeyValueFile::setTableOffset(uint64_t bucket, uint64_t offset) const {
  std::byte* dt = data();
  dt += 100;
  dt += bucket * sizeof(uint64_t);

  std::memcpy(dt, &offset, sizeof(offset));
}

uint64_t KeyValueFile::numBuckets() const {
  uint64_t n;
  std::memcpy(&n, data() + 1 * sizeof(uint64_t), sizeof(n));
  if (n == 0) {
    throw std::runtime_error("numBuckets() zero");
  }
  return n;
}

uint64_t KeyValueFile::nextDataOffset() const {
  uint64_t n;
  std::memcpy(&n, data() + 3 * sizeof(uint64_t), sizeof(n));
  return n;
}

void KeyValueFile::setNextDataOffset(uint64_t off) {
  std::memcpy(data() + 3 * sizeof(uint64_t), &off, sizeof(uint64_t));
}

uint64_t KeyValueFile::numItems() const {
  uint64_t n;
  std::memcpy(&n, data() + 4 * sizeof(uint64_t), sizeof(n));
  return n;
}

void KeyValueFile::setNumItems(uint64_t n) {
  std::memcpy(data() + 4 * sizeof(uint64_t), &n, sizeof(uint64_t));
}

uint64_t KeyValueFile::wasted() const {
  uint64_t n;
  std::memcpy(&n, data() + 2 * sizeof(uint64_t), sizeof(n));
  return n;
}

void KeyValueFile::setWasted(uint64_t w) {
  std::memcpy(data() + 2 * sizeof(uint64_t), &w, sizeof(uint64_t));
}

void KeyValueFile::optimize() {
  locked_ = false;

  double fact = (double)numItems() / (double)numBuckets();
  if (fact > 1.05 || fact < 0.6) {
    uint64_t tabSize = findTabSizePrime(numItems() / 0.8);
    uint64_t contentSize =
        nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
    changeTable(tabSize, contentSize);
    return;
  }

  if (wasted() > 500000) {
    uint64_t contentSize =
        nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
    changeTable(numBuckets(), contentSize);
    return;
  }

  // optimize content
  auto s = nextDataOffset();
  file_.close();
  fs::resize_file(path_, s);
  openFile();
}

void KeyValueFile::lockTableForNumItems(uint64_t n) {
  locked_ = true;

  double fact = (double)n / (double)numBuckets();
  if (fact < 0.9 && fact > 0.6) {
    return;
  }

  auto pth = tmpFilePath();
  uint64_t tabSize = findTabSizePrime(n / 0.8);
  uint64_t contentSize =
      file_.size() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
  changeTable(tabSize, contentSize);
}

void KeyValueFile::unlockTable() { locked_ = false; }

std::vector<std::pair<std::string_view, BytesView>>
KeyValueFile::allDocuments() const {
  std::vector<std::pair<std::string_view, BytesView>> arr;
  arr.reserve(numItems());
  auto numB = numBuckets();
  for (uint64_t i = 0; i < numB; ++i) {
    auto it = firstItem(i);
    while (it.valid()) {
      arr.push_back({it.key(), it.value()});
      it = it.next();
    }
  }
  return arr;
}

void KeyValueFile::ensureFreeSpace(size_t additional) {
  if (buffer_) {
    return;
  }

  auto s = file_.size();
  auto minS = nextDataOffset() + additional;
  if (minS <= s) {
    return;
  }

  file_.close();

  if (s < 3000000) {
    s += 700000;
  } else {
    s += 5000000;
  }
  if (minS > s) {
    s = minS + additional * 0.1;
  }

  fs::resize_file(path_, s);
  openFile();
}

void KeyValueFile::ensureTableSize(int64_t additional) {
  if (locked_) {
    return;
  }

  uint64_t num = numItems() + additional;
  double fact = (double)num / (double)numBuckets();
  if (fact <= 1.4 && fact >= 0.3) {
    return;
  }
  if (fact < 1 && numBuckets() <= 101) {
    return;
  }

  auto pth = tmpFilePath();
  uint64_t tabSize;
  if (fact > 1) {
    // incresing
    tabSize = findTabSizePrimeDouble(num * 1.8);
  } else {
    // decreasing
    tabSize = findTabSizePrimeDouble(num);
  }
  if (tabSize == numBuckets()) {
    return;
  }
  auto contentSize = file_.size() - numBuckets() * sizeof(uint64_t);

  changeTable(tabSize, contentSize);
}

void KeyValueFile::ensureOptimalWaste() {
  if (locked_) {
    return;
  }
  if (wasted() < 30'000'000) {
    return;
  }

  auto pth = tmpFilePath();
  uint64_t contentSize = file_.size() - 100 - numBuckets() * sizeof(uint64_t);
  changeTable(numBuckets(), contentSize);
}

fs::path KeyValueFile::tmpFilePath() const {
  auto pth = path_;
  pth += ".tmp";
  return pth;
}

void KeyValueFile::openFile() {
  file_.open(path_);
  if (!file_.is_open() || file_.data() == nullptr) {
    throw std::runtime_error("Cant open file");
  }

  uint64_t ver;
  // version
  std::memcpy(&ver, file_.data() + 0 * sizeof(uint64_t), sizeof(uint64_t));
  ver = boost::endian::little_to_native<uint64_t>(ver);
  if (ver != Version) {
    file_.close();
    throw std::runtime_error("KeyValueFile::openFile() Different version");
  }
}

bool KeyValueFile::isFileVersionOk(const fs::path& pth) {
  if (!fs::is_regular_file(pth)) {
    return true;
  }
  if (fs::file_size(pth) == 0) {
    return false;
  }
  boost::iostreams::mapped_file f;
  f.open(pth);
  if (!f.is_open() || f.data() == nullptr) {
    throw std::runtime_error("Cant open file");
  }

  if (f.size() < 100) {
    f.close();
    return false;
  }

  uint64_t ver;
  // version
  std::memcpy(&ver, f.data() + 0 * sizeof(uint64_t), sizeof(uint64_t));
  ver = boost::endian::little_to_native<uint64_t>(ver);
  bool ok = ver == Version;
  f.close();
  return ok;
}

void KeyValueFile::changeTable(uint64_t tabSize, uint64_t contentSize) {
  if (buffer_) {
    throw std::runtime_error("cant change table when using buffer");
  }

  // for buffer only use absolutely necesary   -freeSpace -waste
  size_t newSizeContent =
      nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
  auto newSize = 100 + tabSize * sizeof(uint64_t) + newSizeContent;
  bool isEnoughRam = newSize < (availableMemory() - 100000000) * 0.9;

  std::unique_ptr<std::byte[]> buffer2;
  if (isEnoughRam) {
    buffer2.reset(new (std::nothrow) std::byte[newSize]);
  }

  // check if new failed - use disk
  if (!buffer2) {
    auto pth = tmpFilePath();
    createFile(pth, tabSize, contentSize);
    {
      KeyValueFile tmp(pth);
      tmp.locked_ = true;
      tmp.set(*this);
    }
    file_.close();
    fs::rename(pth, path_);
    openFile();
    return;
  }

  {
    std::memset(buffer2.get(), 0, 100 + tabSize * sizeof(uint64_t));

    uint64_t numWasted = 0;
    uint64_t nextDataOffset2 = 100 + tabSize * sizeof(uint64_t);
    uint64_t numItems = 0;

    // version number must allways be in little endian format
    // to future proof for later
    const auto v = boost::endian::native_to_little<uint64_t>(Version);
    // number of slots in hash table
    std::memcpy(buffer2.get() + 0 * sizeof(uint64_t), &v, sizeof(uint64_t));
    // number of slots in hash table
    std::memcpy(buffer2.get() + 1 * sizeof(uint64_t), &tabSize,
                sizeof(uint64_t));
    // number of vasted bytes
    std::memcpy(buffer2.get() + 2 * sizeof(uint64_t), &numWasted,
                sizeof(uint64_t));
    // next data offset
    std::memcpy(buffer2.get() + 3 * sizeof(uint64_t), &nextDataOffset2,
                sizeof(uint64_t));
    // num items
    std::memcpy(buffer2.get() + 4 * sizeof(uint64_t), &numItems,
                sizeof(uint64_t));
  }

  // dont optimize read from disk - no difference

  {
    fs::path p2;
    KeyValueFile tmp(p2);
    tmp.locked_ = true;
    tmp.buffer_ = buffer2.get();
    tmp.set(*this);
  }

  file_.close();

  auto newSize2 = 100 + tabSize * sizeof(uint64_t) + contentSize;
  fs::resize_file(path_, newSize2);

  {
    boost::iostreams::mapped_file file;
    file.open(path_);
    if (!file.is_open()) {
      throw std::runtime_error("Cant open file2");
    }
    assert(file.data());
    std::memcpy(file.data(), buffer2.get(), newSize);
    file.close();
  }

  openFile();
}

void KeyValueFile::clear() {
  file_.close();
  createFile(path_);
  openFile();
}

const size_t primesDoubleForTabSize[] = {
    101,       191,       359,       673,        1249,       2311,
    4283,      7927,      14669,     27143,      50221,      92921,
    171917,    318077,    588463,    1088657,    2014027,    3725951,
    6893011,   12752071,  23591333,  43644023,   80741447,   149371709,
    276337673, 511224709, 945765721, 1749666587, 3236883239, 5988234011};
const size_t primesDoubleForTabSizeNum =
    sizeof(primesDoubleForTabSize) / sizeof(size_t);
const size_t primesForTabSize[] = {
    101,        113,        127,        149,        167,        191,
    211,        233,        257,        283,        313,        347,
    383,        431,        479,        541,        599,        659,
    727,        809,        907,        1009,       1117,       1229,
    1361,       1499,       1657,       1823,       2011,       2213,
    2437,       2683,       2953,       3251,       3581,       3943,
    4339,       4783,       5273,       5801,       6389,       7039,
    7753,       8537,       9391,       10331,      11369,      12511,
    13763,      15149,      16673,      18341,      20177,      22229,
    24469,      26921,      29629,      32603,      35869,      39461,
    43411,      47777,      52561,      57829,      63617,      69991,
    76991,      84691,      93169,      102497,     112757,     124067,
    136481,     150131,     165161,     181693,     199873,     219871,
    241861,     266051,     292661,     321947,     354143,     389561,
    428531,     471389,     518533,     570389,     627433,     690187,
    759223,     835207,     918733,     1010617,    1111687,    1222889,
    1345207,    1479733,    1627723,    1790501,    1969567,    2166529,
    2383219,    2621551,    2883733,    3172123,    3489347,    3838283,
    4222117,    4644329,    5108767,    5619667,    6181639,    6799811,
    7479803,    8227787,    9050599,    9955697,    10951273,   12046403,
    13251047,   14576161,   16033799,   17637203,   19400929,   21341053,
    23475161,   25822679,   28404989,   31245491,   34370053,   37807061,
    41587807,   45746593,   50321261,   55353391,   60888739,   66977621,
    73675391,   81042947,   89147249,   98061979,   107868203,  118655027,
    130520531,  143572609,  157929907,  173722907,  191095213,  210204763,
    231225257,  254347801,  279782593,  307760897,  338536987,  372390691,
    409629809,  450592801,  495652109,  545217341,  599739083,  659713007,
    725684317,  798252779,  878078057,  965885863,  1062474559, 1168722059,
    1285594279, 1414153729, 1555569107, 1711126033, 1882238639, 2070462533,
    2277508787, 2505259681, 2755785653, 3031364227, 3334500667, 3667950739,
    4034745863, 4438220467, 4882042547, 5370246803, 5907271567, 6497998733};
const size_t primesForTabSizeNum = sizeof(primesForTabSize) / sizeof(size_t);
inline size_t KeyValueFile::findTabSizePrime(size_t minNum) {
  for (size_t i = 0; i < primesForTabSizeNum; ++i) {
    if (primesForTabSize[i] > minNum) {
      return primesForTabSize[i];
    }
  }
  throw std::runtime_error("KeyValueFile findTabSizePrime() too big table");
}
inline size_t KeyValueFile::findTabSizePrimeDouble(size_t minNum) {
  for (size_t i = 0; i < primesDoubleForTabSizeNum; ++i) {
    if (primesDoubleForTabSize[i] > minNum) {
      return primesDoubleForTabSize[i];
    }
  }
  throw std::runtime_error(
      "KeyValueFile findTabSizePrimeDouble() too big table");
}

uint64_t KeyValueFile::availableMemory() {
  static uint64_t mem = 0;
  if (mem == 0) {
// for determining ram size
// https://stackoverflow.com/a/2513561
#ifdef _WIN32
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    GlobalMemoryStatusEx(&status);
    mem = status.ullTotalPhys;
#else
    auto pages = sysconf(_SC_PHYS_PAGES);
    auto page_size = sysconf(_SC_PAGE_SIZE);
    mem = pages * page_size;
#endif
    if (mem == 0) {
      throw std::runtime_error("Cant figure out how much ram is installed");
    }
  }
  return mem;
}

} // namespace Search
