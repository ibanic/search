#include "KeyValueFileList.hpp"
#include <city.h>
#include <fstream>
#include <iostream>
#include <thread>
#include <mutex>
#include <cstring>
#include <boost/endian/conversion.hpp>

// for ram
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif



#define makeStringView(str) BytesView(str.data(), str.size())

namespace Search {

	#define BulkReserve 1'000'000  // 1Mb

	const uint64_t KeyValueFileList::Version;

	KeyValueFileList::KeyValueFileList(const fs::path& path)
		: path_(path), locked_(false), buffer_(nullptr), importing_(nullptr)
	{
		if( !path.empty() ) {
			if( !fs::is_regular_file(path_) ) {
				// create empty file
				createFile(path);
			}
			openFile();
		}
	}


	KeyValueFileList::~KeyValueFileList()
	{
		if( importing_ ) {
			delete importing_;
			importing_ = nullptr;
		}
	}


	void KeyValueFileList::createFile(const fs::path& path, uint64_t tabSize, uint64_t contentSize)
	{
		// create
		{ std::ofstream output(path.string()); }
		fs::resize_file(path, 100+tabSize*sizeof(uint64_t)+contentSize);

		boost::iostreams::mapped_file file;
		file.open(path);
		if( !file.is_open() )
			throw std::runtime_error("Cant open file2");
		assert(file.data());

		/*auto ptr2 = reinterpret_cast<uint64_t*>(file.data());
		ptr2[0] = 1;  // version
		ptr2[1] = tabSize;  // number of slots in hash table
		ptr2[2] = 0;  // number of vasted bytes
		ptr2[3] = 100+tabSize*sizeof(uint64_t);  // next data offset
		ptr2[4] = 0;  // num items
		ptr2[5] = 0;  // num keys*/

		uint64_t numWasted = 0;
		uint64_t nextDataOffset2 = 100+tabSize*sizeof(uint64_t);
		uint64_t numItems = 0;
		uint64_t numKeys = 0;
		
		const auto v = boost::endian::native_to_little<uint64_t>(Version);
		std::memcpy(file.data()+0*sizeof(uint64_t), &v, sizeof(uint64_t));
		std::memcpy(file.data()+1*sizeof(uint64_t), &tabSize, sizeof(uint64_t));  // number of slots in hash table
		std::memcpy(file.data()+2*sizeof(uint64_t), &numWasted, sizeof(uint64_t));  // number of wasted bytes
		std::memcpy(file.data()+3*sizeof(uint64_t), &nextDataOffset2, sizeof(uint64_t));  // next data offset
		std::memcpy(file.data()+4*sizeof(uint64_t), &numItems, sizeof(uint64_t));  // num items
		std::memcpy(file.data()+5*sizeof(uint64_t), &numKeys, sizeof(uint64_t));  // num keys


		file.close();
	}


	size_t KeyValueFileList::fileSize() const
	{
		return file_.size();
	}

	bool KeyValueFileList::isFileVersionOk(const fs::path& pth)
	{
		if(!fs::is_regular_file(pth))
			return true;
		if(fs::file_size(pth) == 0)
			return false;
		boost::iostreams::mapped_file f;
		f.open(pth);
		if( !f.is_open() || f.data() == nullptr )
			throw std::runtime_error("Cant open file");
		
		if(f.size() < 100) {
			f.close();
			return false;
		}
		
		uint64_t ver;
		std::memcpy(&ver, f.data()+0*sizeof(uint64_t), sizeof(uint64_t));  // version
		ver = boost::endian::little_to_native<uint64_t>(ver);
		bool ok = ver == Version;
		f.close();
		return ok;
	}


	void KeyValueFileList::set(const std::string& key, const Bytes& value)
	{
		set(std::string_view(key), makeStringView(value));
	}


	void KeyValueFileList::set(std::string_view key, BytesView value)
	{
		ensureTableSize(1);
		auto bucket = calcBucket(key);
		auto itemSize = ItemKey::calcSize(key) + ItemValue::calcSize(value);
		ensureFreeSpace(itemSize);
		setInternal(bucket, key, value);
		ensureOptimalWaste();
	}



	void KeyValueFileList::set(const KeyValueFileList& db2)
	{
		ensureTableSize(db2.numKeys());

		auto num1 = numBuckets();
		auto num2 = db2.numBuckets();
		for( uint64_t i=0; i<num2; ++i ) {
			auto itKey = db2.firstKey(i);
			ensureFreeSpace(itKey.calcSize());
			
			while( itKey.valid() ) {
				uint64_t bucket;
				if( num1 == num2 ) {
					bucket = i;
				}
				else {
					bucket = calcBucket(itKey.key());
				}
				
				auto itValue = itKey.value();
				while(itValue.valid()) {
					ensureFreeSpace(itValue.calcSize());
					setInternal(bucket, itKey.key(), itValue.value());
					itValue = itValue.next();
				}
				itKey = itKey.next();
			}
		}

		ensureOptimalWaste();
	}


	std::vector<BytesView> KeyValueFileList::get(std::string_view key) const
	{
		return getWithBucket(calcBucket(key), key);
	}


	std::vector<BytesView> KeyValueFileList::getWithBucket(uint64_t bucket, std::string_view key) const
	{
		auto itKey = firstKey(bucket);
		while(itKey.valid()) {
			if( itKey.key() == key ) {
				std::vector<BytesView> arr;
				auto itValue = itKey.value();
				while(itValue.valid()) {
					arr.push_back(itValue.value());
					itValue = itValue.next();
				}
				return arr;
			}
			itKey = itKey.next();
		}
		return {};
	}


	std::vector<BytesView> KeyValueFileList::get(const std::string& key2) const
	{
		return get(std::string_view(key2));
	}

	bool KeyValueFileList::exists(std::string_view key, BytesView value) const
	{
		return existsWithBucket(calcBucket(key), key, value);
	}


	bool KeyValueFileList::existsWithBucket(uint64_t bucket, std::string_view key, BytesView value) const
	{
		auto itKey = firstKey(bucket);
		while(itKey.valid()) {
			if( itKey.key() == key ) {
				auto itValue = itKey.value();
				while(itValue.valid()) {
					if( itValue.value() == value ) {
						return true;
					}
					itValue = itValue.next();
				}
				return false;
			}
			itKey = itKey.next();
		}
		return false;
	}


	void KeyValueFileList::remove(std::string_view key, BytesView value)
	{
		auto bucket = calcBucket(key);
		removeInternal(bucket, key, value);
		ensureOptimalWaste();
	}


	void KeyValueFileList::remove(const std::string& key2, const Bytes& value2)
	{
		auto key = std::string_view(key2);
		auto value = makeStringView(value2);
		return remove(key, value);
	}


	void KeyValueFileList::setInternal(uint64_t bucket, std::string_view key, BytesView value)
	{
		uint64_t keyOffset = 0;
		bool valueExists = false;
		auto it = firstKey(bucket);
		while(it.valid()) {
			if( it.key() == key ) {
				keyOffset = it.offset();
				auto it2 = it.value();
				while(it2.valid()) {
					if( it2.value() == value ) {
						valueExists = true;
						break;
					}
					it2 = it2.next();
				}
				break;
			}
			it = it.next();
		}
		if(valueExists) {
			return;
		}

		uint64_t nextValueOffset = keyOffset != 0 ? it.valueOffset() : 0;

		// add value
		auto valueOffset = nextDataOffset();
		auto dt = data() + valueOffset;
		ItemValue::write(dt, nextValueOffset, value);
		setNextDataOffset(dt-data());
		setNumItems(numItems()+1);

		if( keyOffset != 0 ) {
			// key exists
			it.setValueOffset(valueOffset);
		}
		else {
			// key doesnt exist
			keyOffset = nextDataOffset();
			dt = data() + keyOffset;
			ItemKey::write(dt, tableOffset(bucket), key, valueOffset);
			setNextDataOffset(dt-data());
			setNumKeys(numKeys()+1);
			setTableOffset(bucket, keyOffset);
		}
	}


	void KeyValueFileList::removeInternal(uint64_t bucket, std::string_view key, BytesView value)
	{
		// find key
		size_t prevKeyOffset = 0;
		auto itKey = firstKey(bucket);
		while( true ) {
			if( !itKey.valid() ) {
				return;
			}
			if( itKey.key() == key ) {
				break;
			}
			prevKeyOffset = itKey.offset();
			itKey = itKey.next();
		}

		// find value
		auto prevValueOffset = 0;
		auto itValue = itKey.value();
		while( true ) {
			if( !itValue.valid() ) {
				return;
			}
			if( itValue.value() == value ) {
				break;
			}
			prevValueOffset = itValue.offset();
			itValue = itValue.next();
		}

		// remove value
		if( prevValueOffset != 0 ) {
			ItemValue itPrev(data(), prevValueOffset);
			itPrev.setNextOffset(itValue.nextOffset());
		}
		else {
			itKey.setValueOffset(itValue.nextOffset());
		}
		setWasted(wasted() + itValue.calcSize());
		setNumItems(numItems()-1);

		// does key have any more values?
		if( itKey.value().valid() ) {
			return;
		}

		// remove key
		if( prevKeyOffset == 0 ) {
			setTableOffset(bucket, itKey.nextOffset());
		}
		else {
			ItemKey it2(data(), prevKeyOffset);
			it2.setNextOffset(itKey.nextOffset());
		}
		setWasted(wasted() + itKey.calcSize());
		setNumKeys(numKeys()-1);
	}


	KeyValueFileList::ItemKey KeyValueFileList::firstKey(uint64_t bucket) const
	{
		//assert(bucket < numBuckets());
		auto offset = tableOffset(bucket);
		return ItemKey(data(), offset);
	}


	void KeyValueFileList::bulkWrite(std::ofstream& out, std::string_view key, BytesView value)
	{
		uint64_t hash = calcHash(key);
		out.write((const char*)&hash, sizeof(hash));

		// key len
		auto len = ItemKey::calcSize(key);
		auto len2 = writeSizeString(len);
		out.write((const char*)len2.data(), len2.size());

		// key
		Bytes buff(len, (std::byte)'\0');
		std::byte* buff2 = &buff[0];
		ItemKey::write(buff2, 0, key, 0);
		out.write((const char*)buff.data(), buff.size());

		// value len
		len = ItemValue::calcSize(value);
		len2 = writeSizeString(len);
		out.write((const char*)len2.data(), len2.size());

		// value
		buff = Bytes(len, (std::byte)'\0');
		buff2 = &buff[0];
		ItemValue::write(buff2, 0, value);
		out.write((const char*)buff.data(), buff.size());
	}


	std::tuple<uint64_t, std::string_view, BytesView> KeyValueFileList::bulkRead(const std::byte*& dt)
	{
		uint64_t hash;
		std::memcpy(&hash, dt, sizeof(hash));
		dt += sizeof(hash);

		auto len = readSize(dt);
		std::byte* dt2 = (std::byte*)dt;
		ItemKey itKey(dt2, 0);
		dt += len;

		len = readSize(dt);
		dt2 = (std::byte*)dt;
		ItemValue itValue(dt2, 0);
		dt += len;

		return {hash, itKey.key(), itValue.value()};
	}


	void KeyValueFileList::bulkStart(size_t numThreads)
	{
		assert(!importing_);
		std::unique_ptr<ImportingData> dt(new ImportingData());
		dt->numItems = numItems();
		dt->numKeys = numKeys();
		dt->wasted = wasted();
		dt->dataRange.resize(numThreads, {0,0});
		importing_ = dt.release();
	}
	void KeyValueFileList::bulkStop()
	{
		assert(importing_);

		for(auto& pair : importing_->dataRange) {
			auto diff = pair.second - pair.first;
			importing_->wasted += diff;
		}
		
		std::unique_ptr<ImportingData> dt(importing_);
		importing_ = nullptr;
		setNumItems(dt->numItems);
		setNumKeys(dt->numKeys);
		setWasted(dt->wasted);
	}


	void KeyValueFileList::bulkInsertEnlarge(size_t nthThread, size_t numThreads)
	{
		// must be locked by caller
		//std::unique_lock<std::shared_mutex> lock2(importing_->mutex_);

		auto diff = importing_->dataRange[nthThread].second - importing_->dataRange[nthThread].first;
		importing_->wasted += diff;
		importing_->dataRange[nthThread].first = 0;
		importing_->dataRange[nthThread].second = 0;

		setWasted(importing_->wasted);
		setNumItems(importing_->numItems);
		setNumKeys(importing_->numKeys);
		if( wasted() > 100'000'000 ) {
			for(auto& pair : importing_->dataRange) {
				auto diff = pair.second - pair.first;
				if(diff > 0) {
					setWasted(wasted() + diff);
				}
				pair.first = 0;
				pair.second = 0;
			}

			uint64_t contentSize = nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
			changeTable(numBuckets(), contentSize);
			importing_->wasted = wasted();
			assert(importing_->numItems == numItems());
			assert(importing_->numKeys == numKeys());
		}
		
		// ensure free space
		ensureFreeSpace(BulkReserve);
		importing_->dataRange[nthThread].first = nextDataOffset();
		importing_->dataRange[nthThread].second = importing_->dataRange[nthThread].first + BulkReserve;
		setNextDataOffset(importing_->dataRange[nthThread].second);
	}


	void KeyValueFileList::bulkInsert(uint64_t bucket, std::string_view key, BytesView value, size_t nthThread, size_t numThreads)
	{
		auto itemSize = ItemKey::calcSize(key) + ItemValue::calcSize(value);
		if( itemSize > BulkReserve ) {
			throw std::runtime_error("Too big");
		}

		std::lock_guard lock1(importing_->mutex_);
		if( itemSize > importing_->dataRange[nthThread].second - importing_->dataRange[nthThread].first ) {
			bulkInsertEnlarge(nthThread, numThreads);
		}


		uint64_t keyOffset = 0;
		bool valueExists = false;
		auto it = firstKey(bucket);
		while(it.valid()) {
			if( it.key() == key ) {
				keyOffset = it.offset();
				/*auto it2 = it.value();
				while(it2.valid()) {
					if( it2.value() == value ) {
						valueExists = true;
						break;
					}
					it2 = it2.next();
				}*/
				break;
			}
			it = it.next();
		}
		if(valueExists) {
			return;
		}

		uint64_t nextValueOffset = keyOffset != 0 ? it.valueOffset() : 0;

		// add value
		auto valueOffset = importing_->dataRange[nthThread].first;
		auto dt = data() + valueOffset;
		ItemValue::write(dt, nextValueOffset, value);
		importing_->dataRange[nthThread].first += dt - data() - valueOffset;
		importing_->numItems++;

		if( keyOffset != 0 ) {
			// key exists
			it.setValueOffset(valueOffset);
		}
		else {
			// key doesnt exist
			keyOffset = importing_->dataRange[nthThread].first;
			dt = data() + keyOffset;
			ItemKey::write(dt, tableOffset(bucket), key, valueOffset);
			importing_->dataRange[nthThread].first += dt - data() - keyOffset;
			importing_->numKeys++;
			setTableOffset(bucket, keyOffset);
		}
	}


	void KeyValueFileList::bulkRemove(uint64_t bucket, std::string_view key, BytesView value, size_t nthThread, size_t numThreads)
	{
		std::lock_guard lock1(importing_->mutex_);

		// find key
		size_t prevKeyOffset = 0;
		auto itKey = firstKey(bucket);
		while( true ) {
			if( !itKey.valid() ) {
				return;
			}
			if( itKey.key() == key ) {
				break;
			}
			prevKeyOffset = itKey.offset();
			itKey = itKey.next();
		}

		// find value
		auto prevValueOffset = 0;
		auto itValue = itKey.value();
		while( true ) {
			if( !itValue.valid() ) {
				return;
			}
			if( itValue.value() == value ) {
				break;
			}
			prevValueOffset = itValue.offset();
			itValue = itValue.next();
		}

		// remove value
		if( prevValueOffset != 0 ) {
			ItemValue itPrev(data(), prevValueOffset);
			itPrev.setNextOffset(itValue.nextOffset());
		}
		else {
			itKey.setValueOffset(itValue.nextOffset());
		}
		importing_->wasted += itValue.calcSize();
		importing_->numItems--;

		// does key have any more values?
		if( itKey.value().valid() ) {
			return;
		}

		// remove key
		if( prevKeyOffset == 0 ) {
			setTableOffset(bucket, itKey.nextOffset());
		}
		else {
			ItemKey it2(data(), prevKeyOffset);
			it2.setNextOffset(itKey.nextOffset());
		}
		importing_->wasted += itKey.calcSize();
		importing_->numKeys--;
		
		/*size_t offset = 0;
		size_t prevOffset = 0;
		bool otherKeys = false;
		{
			auto it = firstKey(bucket);
			while(it.valid()) {
				if( it.key() == key ) {
					if(it.value() == value) {
						offset = it.offset();
					}
					else {
						otherKeys = true;
					}
					if( offset != 0 && otherKeys ) {
						break;
					}
				}
				prevOffset = it.offset();
				it = it.next();
			}
		}

		if(offset == 0) {
			return;
		}

		Item it3(data(), offset);
		if( prevOffset == 0 ) {
			setTableOffset(bucket, it3.nextOffset());
		}
		else {
			Item it2(data(), prevOffset);
			it2.setNextOffset(it3.nextOffset());
		}
		importing_->numItems--;
		importing_->wasted += it3.calcSize();*/
	}


	bool KeyValueFileList::bulkIsInThread(uint64_t bucket, size_t nthThread, size_t numThreads, uint64_t numBuckets2)
	{
		if( numThreads == 1 )
			return true;
		size_t perThread = numBuckets2 / numThreads;
		size_t start = nthThread * perThread;
		if( bucket < start )
			return false;
		if( bucket < start + perThread )
			return true;
		if( nthThread+1 == numThreads ) {
			return true;
		}
		return false;
	}


	uint64_t KeyValueFileList::calcHash(std::string_view key)
	{
		return CityHash64((const char*)key.data(), key.size());
	}
	
	
	uint64_t KeyValueFileList::calcBucketFromHash(uint64_t hash, uint64_t numBuckets2)
	{
		return hash % numBuckets2;
	}


	uint64_t KeyValueFileList::calcBucket(std::string_view key) const
	{
		return calcBucketFromHash(calcHash(key), numBuckets());
	}


	/*std::vector<uint64_t> KeyValueFileList::calcBuckets(const std::vector<std::pair<std::string_view, BytesView>>& arr) const
	{
		std::vector<uint64_t> buckets(arr.size());
		for( size_t i=0; i<arr.size(); ++i ) {
			buckets[i] = calcBucket(arr[i].first);
		}
		return buckets;
	}*/


	std::byte* KeyValueFileList::data() const
	{
		//assert(file_.is_open());
		if( buffer_ )
			return buffer_;
		std::byte* dt = (std::byte*)file_.data();
		//assert(dt != nullptr);
		return dt;
	}


	uint64_t KeyValueFileList::tableOffset(uint64_t bucket) const
	{
		std::byte* dt = data();
		dt += 100;
		dt += bucket * sizeof(uint64_t);
		//return *reinterpret_cast<uint64_t*>(dt);
		uint64_t n; std::memcpy(&n, dt, sizeof(n)); return n;
	}
	void KeyValueFileList::setTableOffset(uint64_t bucket, uint64_t offset) const
	{
		std::byte* dt = data();
		dt += 100;
		dt += bucket * sizeof(uint64_t);
		//*reinterpret_cast<uint64_t*>(dt) = offset;
		std::memcpy(dt, &offset, sizeof(uint64_t));
	}


	uint64_t KeyValueFileList::numBuckets() const
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//return ptr2[0];
		uint64_t n; std::memcpy(&n, data()+1*sizeof(uint64_t), sizeof(n)); return n;
	}


	uint64_t KeyValueFileList::nextDataOffset() const
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//return ptr2[2];
		uint64_t n; std::memcpy(&n, data()+3*sizeof(uint64_t), sizeof(n)); return n;
	}


	void KeyValueFileList::setNextDataOffset(uint64_t off)
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//ptr2[2] = off;
		std::memcpy(data()+3*sizeof(uint64_t), &off, sizeof(uint64_t));
	}


	uint64_t KeyValueFileList::numItems() const
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//return ptr2[3];
		uint64_t n; std::memcpy(&n, data()+4*sizeof(uint64_t), sizeof(n)); return n;
	}


	void KeyValueFileList::setNumItems(uint64_t n)
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//ptr2[3] = n;
		std::memcpy(data()+4*sizeof(uint64_t), &n, sizeof(uint64_t));
	}


	uint64_t KeyValueFileList::numKeys() const
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//return ptr2[4];
		uint64_t n; std::memcpy(&n, data()+5*sizeof(uint64_t), sizeof(n)); return n;
	}


	void KeyValueFileList::setNumKeys(uint64_t n)
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//ptr2[4] = n;
		std::memcpy(data()+5*sizeof(uint64_t), &n, sizeof(uint64_t));
	}


	uint64_t KeyValueFileList::wasted() const
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//return ptr2[1];
		uint64_t n; std::memcpy(&n, data()+2*sizeof(uint64_t), sizeof(n)); return n;
	}


	void KeyValueFileList::setWasted(uint64_t w)
	{
		//auto ptr2 = reinterpret_cast<uint64_t*>(data());
		//ptr2[1] = w;
		std::memcpy(data()+2*sizeof(uint64_t), &w, sizeof(uint64_t));
	}


	void KeyValueFileList::optimize()
	{
		locked_ = false;

		double fact = (double)numKeys() / (double)numBuckets();
		if( fact > 1.05 || fact < 0.6 ) {
			uint64_t tabSize = findTabSizePrime(numKeys()/0.8);
			uint64_t contentSize = nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
			changeTable(tabSize, contentSize);
			return;
		}

		if( wasted() > 500000 ) {
			uint64_t contentSize = nextDataOffset() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
			changeTable(numBuckets(), contentSize);
			return;
		}

		// optimize content
		auto s = nextDataOffset();
		file_.close();
		fs::resize_file(path_, s);
		openFile();
	}


	void KeyValueFileList::lockTableForNumKeys(uint64_t n)
	{
		locked_ = true;

		double fact = (double)n / (double)numBuckets();
		if( fact < 0.9 && fact > 0.6 ) {
			return;
		}
		
		auto pth = tmpFilePath();
		uint64_t tabSize = findTabSizePrime(n/0.8);
		uint64_t contentSize = file_.size() - 100 - numBuckets() * sizeof(uint64_t) - wasted();
		changeTable(tabSize, contentSize);
	}


	void KeyValueFileList::unlockTable()
	{
		locked_ = false;
	}


	std::vector<std::pair<std::string_view,BytesView>> KeyValueFileList::allDocuments() const
	{
		std::vector<std::pair<std::string_view,BytesView>> arr;
		arr.reserve(numItems());
		auto numB = numBuckets();
		for( uint64_t i=0; i<numB; ++i ) {
			auto itKey = firstKey(i);
			while( itKey.valid() ) {
				auto itValue = itKey.value();
				while(itValue.valid()) {
					arr.push_back({itKey.key(), itValue.value()});
					itValue = itValue.next();
				}
				itKey = itKey.next();
			}
		}
		return arr;
	}


	void KeyValueFileList::ensureFreeSpace(size_t additional)
	{
		if( buffer_ ) {
			return;
			//throw std::runtime_error("cant ensureFreeSpace() when using buffer");
		}
		
		auto s = file_.size();
		auto minS = nextDataOffset() + additional;
		if( minS <= s ) {
			return;
		}

		file_.close();

		if( s < 3000000 ) {
			s += 700000;
		}
		else {
			s += 5000000;
		}
		if( minS > s ) {
			s = minS + additional * 0.1;
		}

		fs::resize_file(path_, s);
		openFile();
	}


	void KeyValueFileList::ensureTableSize(int64_t additional)
	{
		if( locked_ )
			return;

		uint64_t num = numKeys() + additional;
		double fact = (double)num / (double)numBuckets();
		if( fact <= 1.4 && fact >= 0.3 ) {
			return;
		}
		if( fact < 1 && numBuckets() <= 101 ) {
			return;
		}

		auto pth = tmpFilePath();
		uint64_t tabSize;
		if( fact > 1 ) {
			// incresing
			tabSize = findTabSizePrimeDouble(num*1.8);
		}
		else {
			// decreasing
			tabSize = findTabSizePrimeDouble(num);
		}
		if( tabSize == numBuckets() ) {
			return;
		}
		auto contentSize = file_.size() - numBuckets() * sizeof(uint64_t);

		changeTable(tabSize, contentSize);
	}


	void KeyValueFileList::ensureOptimalWaste()
	{
		if( locked_ )
			return;
		if( wasted() < 30'000'000 )
			return;
		
		auto pth = tmpFilePath();
		uint64_t contentSize = file_.size() - 100 - numBuckets() * sizeof(uint64_t);
		changeTable(numBuckets(), contentSize);
	}


	fs::path KeyValueFileList::tmpFilePath() const
	{
		auto pth = path_;
		pth += ".tmp";
		return pth;
	}

	void KeyValueFileList::openFile()
	{
		file_.open(path_);
		if( !file_.is_open() || file_.data() == nullptr )
			throw std::runtime_error("Cant open file");
			
		uint64_t ver;
		std::memcpy(&ver, file_.data()+0*sizeof(uint64_t), sizeof(uint64_t));  // version
		ver = boost::endian::little_to_native<uint64_t>(ver);
		if(ver != Version) {
			file_.close();
			throw std::runtime_error("KeyValueFileList::openFile() Different version");
		}
	}


	void KeyValueFileList::changeTable(uint64_t tabSize, uint64_t contentSize)
	{
		if( buffer_ ) {
			throw std::runtime_error("cant change table when using buffer");
		}
		
		// for buffer only use absolutely necesary   -freeSpace -waste
		size_t newSizeContent = nextDataOffset() - 100 - numBuckets()*sizeof(uint64_t) - wasted();
		auto newSize = 100+tabSize*sizeof(uint64_t)+newSizeContent;
		bool isEnoughRam = newSize < (availableMemory()-100000000)*0.9;
		
		std::unique_ptr<std::byte[]> buffer2;
		if( isEnoughRam ) {
			buffer2.reset(new (std::nothrow) std::byte[newSize]);
		}

		// check if new failed - use disk
		if( !buffer2 ) {
			auto pth = tmpFilePath();
			createFile(pth, tabSize, contentSize);
			{
				KeyValueFileList tmp(pth);
				tmp.locked_ = true;
				tmp.set(*this);
			}
			file_.close();
			fs::rename(pth, path_);
			openFile();
			return;
		}

		{
			std::memset(buffer2.get(), 0, 100+tabSize*sizeof(uint64_t));
			/*auto ptr2 = reinterpret_cast<uint64_t*>(buffer2.get());
			ptr2[0] = 1;  // version
			ptr2[1] = tabSize;  // number of slots in hash table
			ptr2[2] = 0;  // number of wasted bytes
			ptr2[3] = 100+tabSize*sizeof(uint64_t);  // next data offset
			ptr2[4] = 0;  // num items
			ptr2[5] = 0;  // num keys*/

			uint64_t numWasted = 0;
			uint64_t nextDataOffset2 = 100+tabSize*sizeof(uint64_t);
			uint64_t numItems = 0;
			uint64_t numKeys = 0;
			
			const auto v = boost::endian::native_to_little<uint64_t>(Version);
			std::memcpy(buffer2.get()+0*sizeof(uint64_t), &v, sizeof(uint64_t));
			std::memcpy(buffer2.get()+1*sizeof(uint64_t), &tabSize, sizeof(uint64_t));  // number of slots in hash table
			std::memcpy(buffer2.get()+2*sizeof(uint64_t), &numWasted, sizeof(uint64_t));  // number of wasted bytes
			std::memcpy(buffer2.get()+3*sizeof(uint64_t), &nextDataOffset2, sizeof(uint64_t));  // next data offset
			std::memcpy(buffer2.get()+4*sizeof(uint64_t), &numItems, sizeof(uint64_t));  // num items
			std::memcpy(buffer2.get()+5*sizeof(uint64_t), &numKeys, sizeof(uint64_t));  // num keys
		}

		// dont optimize read from disk - no difference

		{
			fs::path p2;
			KeyValueFileList tmp(p2);
			tmp.locked_ = true;
			tmp.buffer_ = buffer2.get();
			tmp.set(*this);
		}

		file_.close();
		
		auto newSize2 = 100+tabSize*sizeof(uint64_t)+contentSize;
		fs::resize_file(path_, newSize2);

		{
			boost::iostreams::mapped_file file;
			file.open(path_);
			if( !file.is_open() )
				throw std::runtime_error("Cant open file2");
			assert(file.data());
			std::memcpy(file.data(), buffer2.get(), newSize);
			file.close();
		}
		
		openFile();
	}

	void KeyValueFileList::clear()
	{
		file_.close();
		createFile(path_);
		openFile();
	}




/*
#include <primesieve.hpp>
#include <iostream>

// brew install primesieve
// clang++ primes.cc -lprimesieve
// ./a.out

int main()
{
	// https://github.com/kimwalisch/primesieve#c-api

	primesieve::iterator it;
	uint64_t prime = it.next_prime();

	// iterate over the primes below 10^6
	size_t last = 1;
	for (; prime < 7000000000; prime = it.next_prime())
	{
		if( prime > 100 && (double)prime/last > 1.85 )
		{
			last = prime;
			std::cout << prime << ",";
		}
	}

	return 0;
}*/
	const size_t primesDoubleForTabSize[] = { 101,191,359,673,1249,2311,4283,7927,14669,27143,50221,92921,171917,318077,588463,1088657,2014027,3725951,6893011,12752071,23591333,43644023,80741447,149371709,276337673,511224709,945765721,1749666587,3236883239,5988234011 };
	const size_t primesDoubleForTabSizeNum = sizeof(primesDoubleForTabSize)/sizeof(size_t);
	const size_t primesForTabSize[] = {
		101,113,127,149,167,191,211,233,257,283,313,347,383,431,479,541,599,659,727,809,907,1009,1117,1229,1361,1499,1657,1823,2011,2213,2437,2683,2953,3251,3581,3943,4339,4783,5273,5801,6389,7039,7753,8537,9391,10331,11369,12511,13763,15149,16673,18341,20177,22229,24469,26921,29629,32603,35869,39461,43411,47777,52561,57829,63617,69991,76991,84691,93169,102497,112757,124067,136481,150131,165161,181693,199873,219871,241861,266051,292661,321947,354143,389561,428531,471389,518533,570389,627433,690187,759223,835207,918733,1010617,1111687,1222889,1345207,1479733,1627723,1790501,1969567,2166529,2383219,2621551,2883733,3172123,3489347,3838283,4222117,4644329,5108767,5619667,6181639,6799811,7479803,8227787,9050599,9955697,10951273,12046403,13251047,14576161,16033799,17637203,19400929,21341053,23475161,25822679,28404989,31245491,34370053,37807061,41587807,45746593,50321261,55353391,60888739,66977621,73675391,81042947,89147249,98061979,107868203,118655027,130520531,143572609,157929907,173722907,191095213,210204763,231225257,254347801,279782593,307760897,338536987,372390691,409629809,450592801,495652109,545217341,599739083,659713007,725684317,798252779,878078057,965885863,1062474559,1168722059,1285594279,1414153729,1555569107,1711126033,1882238639,2070462533,2277508787,2505259681,2755785653,3031364227,3334500667,3667950739,4034745863,4438220467,4882042547,5370246803,5907271567,6497998733
	};
	const size_t primesForTabSizeNum = sizeof(primesForTabSize)/sizeof(size_t);
	inline size_t KeyValueFileList::findTabSizePrime(size_t minNum)
	{
		for( size_t i=0; i<primesForTabSizeNum; ++i )
		{
			if( primesForTabSize[i] > minNum )
				return primesForTabSize[i];
		}
		throw std::runtime_error("KeyValueFileList findTabSizePrime() too big table");
	}
	inline size_t KeyValueFileList::findTabSizePrimeDouble(size_t minNum)
	{
		for( size_t i=0; i<primesDoubleForTabSizeNum; ++i )
		{
			if( primesDoubleForTabSize[i] > minNum ) {
				return primesDoubleForTabSize[i];
			}
		}
		throw std::runtime_error("KeyValueFileList findTabSizePrimeDouble() too big table");
	}


	uint64_t KeyValueFileList::availableMemory()
	{
		static uint64_t mem = 0;
		if( mem == 0 ) {
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
			if( mem == 0 )
				throw std::runtime_error("Cant figure out how much ram is installed");
		}
		return mem;
	}


}
