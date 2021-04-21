//
//  KeyValueFile.hpp
//
//  Created by Ignac Banic on 11/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <map>
#include <vector>
#include <filesystem>
#include <boost/iostreams/device/mapped_file.hpp>
#include <optional>
#include <string_view>
#include <climits>
#include "CompressSize.hpp"
#include <fstream>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include "Types.hpp"
#include <cstring>


// change that file supports both platforms:
// BOOST_BIG_ENDIAN   BOOST_ENDIAN_INTRINSIC_BYTE_SWAP_4()


namespace fs =  std::filesystem; //boost::filesystem;

namespace Search {
	


	class KeyValueFile {
	public:
		static const uint64_t Version = 1;
		
	private:
		boost::iostreams::mapped_file file_;
		fs::path path_;
		bool locked_;
		std::byte* buffer_;

		struct ImportingData {
			std::atomic<uint64_t> numItems;
			std::atomic<uint64_t> wasted;
			std::vector<std::pair<size_t, size_t>> dataRange;
			std::mutex mutex_;
		};
		ImportingData* importing_;

		class Item {
		private:
			std::byte* data_;
			uint64_t offset_;

		public:
			Item() : data_(nullptr), offset_(0) {}
			Item(std::byte* data, uint64_t offset)
				: data_(data), offset_(offset)
			{ }
			bool valid() const { return offset_ != 0; }
			uint64_t offset() const { return offset_; }
			uint64_t nextOffset() const {
				//return *reinterpret_cast<const uint64_t*>(itemData());
				uint64_t n; std::memcpy(&n, itemData(), sizeof(n)); return n;
			}
			void setNextOffset(uint64_t nextOffset) {
				std::memcpy(itemData(), &nextOffset, sizeof(uint64_t));
			}
			std::string_view key() const
			{
				const std::byte* ptr = itemData() + sizeof(uint64_t);
				auto sKey = readSize(ptr);
				/*auto sValue =*/ readSize(ptr);
				return {(const char*)ptr, sKey};
			}
			BytesView value() const {
				const std::byte* ptr = itemData() + sizeof(uint64_t);
				auto sKey = readSize(ptr);
				auto sValue = readSize(ptr);
				ptr += sKey;
				return BytesView(ptr, sValue);
			}
			void setValue(BytesView txt) {
				const std::byte* ptr = itemData() + sizeof(uint64_t);
				auto sKey = readSize(ptr);
				const std::byte* ptrValSize = ptr;
				auto sValue = readSize(ptr);
				ptr += sKey;

				std::memcpy(const_cast<std::byte*>(ptr), txt.data(), txt.size());
				auto numBytes = numBytesSize(sValue);
				std::byte* ptr2 = const_cast<std::byte*>(ptrValSize);
				writeSize(ptr2, txt.size(), numBytes);
			}
			size_t calcSize() const {
				const std::byte* ptr = itemData() + sizeof(uint64_t);
				auto sKey = readSize(ptr);
				auto sValue = readSize(ptr);
				return ptr - itemData() + sKey + sValue;
			}
			static size_t calcSize(std::string_view key, BytesView value) {
				return sizeof(uint64_t) + numBytesSize(key.size()) + numBytesSize(value.size()) + key.size() + value.size();
			}
			static void write(std::byte*& buffer, uint64_t nextOffset, std::string_view key, BytesView value) {
				std::memcpy(buffer, &nextOffset, sizeof(uint64_t));
				buffer += sizeof(uint64_t);
				writeSize(buffer, key.size());
				writeSize(buffer, value.size());
				std::memcpy(buffer, key.data(), key.size());
				buffer += key.size();
				std::memcpy(buffer, value.data(), value.size());
				buffer += value.size();
			}
			Item next() const {
				return Item(data_, nextOffset());
			}

		private:
			std::byte* itemData() const {
				return data_ + offset_;
			}
		};

	public:
		KeyValueFile(const fs::path& path);
		~KeyValueFile();
		KeyValueFile(const KeyValueFile&) = delete;
		KeyValueFile& operator=(const KeyValueFile&) = delete;
		KeyValueFile(KeyValueFile&&) = default;
		KeyValueFile& operator=(KeyValueFile&&) = default;

		void set(const std::string& key, const Bytes& value);
		void set(std::string_view key, BytesView value);
		//void setBatch(const std::vector<std::pair<Bytes, Bytes>>& arr);
		//void setBatch(const std::vector<std::pair<BytesView, BytesView>>& arr);
		void set(const KeyValueFile& db2);
		BytesView get(const std::string& key) const;
		BytesView get(std::string_view key) const;
		BytesView getWithBucket(uint64_t bucket, std::string_view key) const;
		void remove(std::string_view key);
		void remove(const std::string& key);
		void optimize();
		void lockTableForNumItems(uint64_t n);
		void unlockTable();
		std::vector<std::pair<std::string_view, BytesView>> allDocuments() const;

		const fs::path& path() const { return path_; }
		static void createFile(const fs::path& path, uint64_t tabSize=101, uint64_t contentSize=1000);
		size_t fileSize() const;
		static bool isFileVersionOk(const fs::path& pth);

		static void bulkWrite(std::ofstream& out, std::string_view key, BytesView value);
	private:
		void bulkInsertEnlarge(size_t nthThread, size_t numThreads);
	public:
		void bulkInsert(uint64_t bucket, std::string_view key, BytesView value, size_t nthThread, size_t numThreads);
		void bulkRemove(uint64_t bucket, std::string_view key, BytesView value, size_t nthThread, size_t numThreads);
		static std::tuple<uint64_t, std::string_view, BytesView> bulkRead(const std::byte*& dt);
		static bool bulkIsInThread(uint64_t bucket, size_t nthThread, size_t numThreads, uint64_t numBuckets);
		void bulkStart(size_t numThreads);
		void bulkStop();
		//Bytes getWithBucketImport(uint64_t bucket, std::string_view key) const;

		static uint64_t calcHash(std::string_view key);
		static uint64_t calcBucketFromHash(uint64_t hash, uint64_t numBuckets);
		uint64_t calcBucket(std::string_view key) const;
		//std::vector<uint64_t> calcBuckets(const std::vector<std::pair<std::string_view, BytesView>>& arr) const;
		uint64_t numBuckets() const;
		void ensureOptimalWaste();
		uint64_t numItems() const;
		uint64_t wasted() const;
		void clear();

	private:
		void setInternal(uint64_t bucket, std::string_view key, BytesView value);
		void removeInternal(uint64_t bucket, std::string_view key);
		Item firstItem(uint64_t bucket) const;
		std::pair<uint64_t, Item> findInternal(uint64_t bucket, std::string_view key) const;

		
		std::byte* data() const;
		uint64_t tableOffset(uint64_t bucket) const;
		void setTableOffset(uint64_t bucket, uint64_t offset) const;

		
		uint64_t nextDataOffset() const;
		void setNextDataOffset(uint64_t off);
		
		void setNumItems(uint64_t num);
		void setWasted(uint64_t w);

		void ensureFreeSpace(size_t additional);
		void ensureTableSize(int64_t additional);
		fs::path tmpFilePath() const;
		inline static size_t findTabSizePrime(size_t minNum);
		inline static size_t findTabSizePrimeDouble(size_t minNum);
		void openFile();
		static uint64_t availableMemory();
		void changeTable(uint64_t numBuckets, uint64_t bodySize);
	};
}
