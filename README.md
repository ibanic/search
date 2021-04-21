# Search library
See example for indexing and searching Wikipedia.

## Features

- C++17
- Memory maped files
- Multithreaded
- Custom comparators

## Installation MacOS
LibSearch requires ICU, Boost, Tessil/hopscotch-map and google/cityhash

```sh
brew install boost icu4c cityhash
```

## Compiling
```sh
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/usr/local/opt/icu4c ..
make
```

## Usage
```cpp
#include "Search/FileStore.hpp"
#include "Search/MemoryStore.hpp"
#include "Search/Db.hpp"
#include "Search/FindMany.hpp"
#include "Search/Comparators.hpp"

// open database
typedef Db<FileStore<DocSimple>> TSearchDb;
FileStore<DocSimple> store(pathToDbFiles);
TSearchDb db(store);

// modify
db.add(DocSimple{1, "banana"});
db.remove(1);

// search
typedef Result<Doc> TRes;
CompIsWhole<TRes> cmp1;
CompWordsTogether<TRes> cmp2;

SearchSettings<DocSimple> sett;
sett.query = "banana";
auto results3 = findMany<TSearchDb> ({&db}, sett, cmp1, cmp2);
```

## Custom document class
See DocSimple.hpp for full implementation.
TId can be any type eg. int, std::array, std::string
```cpp
// minimal required class definition
class Doc {
public:
	// must define types
	typedef uint32_t TId;
	typedef std::array<std::byte, sizeof(TId)> TIdSerialized;

public:
	// ctor for deserialization
	Doc(TId id, BytesView dt2);
	
	// serialization
	Bytes serialize() const;
	static TIdSerialized serializeId(TId id);
	static TId deserializeId(const TIdSerialized& arr);
	
	// data access
	TId docId() const;
	std::vector<std::string> allTexts() const;
};
```


## Custom comparator
Example implementation for comparator to sort based on time.
init() and clean() are here to help with bulk initialization if needed.
```cpp
class CompLatest {
public:
	void init(const std::vector<TRes>& all, const Search::SearchSettings<typename TRes::TDoc>& sett) {}
	void clean() { }
	int compare(const TRes& d1, const TRes& d2) const {
		if(d1.doc.time==0 || d2.doc.time==0)
			return 0;
		return d2.doc.time - d1.doc.time;
	}
};
```

## Multithreaded bulk import
For fast data import into DB, there are bulkWriters() and bulkAdd() methods. See full example in test.
```cpp
FileStore<DocSimple> store(output);
TSearchDb db(store);
auto writers = db.bulkWriters(numThreads);
std::vector<std::future<void>> arr(numThreads);
for( size_t i=0; i<numThreads; ++i ) {
	Db<FileStore<DocSimple>>::BulkWriter& writer = writers[i];
	arr[i] = std::async(std::launch::async, [&writer](){
		while( /* hasLines */ ) {
			writer.add(DocSimple{id, txt});
		}
	});
}
for(auto& ft : arr)
	ft.wait();
db.bulkAdd(writers);
```

## License
MIT
