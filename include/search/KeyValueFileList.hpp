//
//  KeyValueFileList.hpp
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


namespace fs =  std::filesystem; //boost::filesystem;


namespace Search {

	class KeyValueFileList {public:
		static const uint64_t Version = 1;
		
	private:
		boost::iostreams::mapped_file file_;
		fs::path path_;
		bool locked_;
		std::byte* buffer_;

		struct ImportingData {
			std::atomic<uint64_t> numItems;
			std::atomic<uint64_t> numKeys;
			std::atomic<uint64_t> wasted;
			std::vector<std::pair<size_t, size_t>> dataRange;
			std::mutex mutex_;
		};
		ImportingData* importing_;

		class ItemValue {
		private:
			std::byte* data_;
			uint64_t offset_;

			// item layout:
			// nextOffset(8), valueSize(1-8), value(...) 

		public:
			ItemValue() : data_(nullptr), offset_(0) {}
			ItemValue(std::byte* data, uint64_t offset)
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
			BytesView value() const {
				const std::byte* ptr = itemData() + sizeof(uint64_t);
				auto sValue = readSize(ptr);
				return {ptr, sValue};
			}
			size_t calcSize() const {
				const std::byte* ptr = itemData() + sizeof(uint64_t);
				auto sValue = readSize(ptr);
				return ptr - itemData() + sValue;
			}
			static size_t calcSize(BytesView value) {
				return sizeof(uint64_t) + numBytesSize(value.size()) + value.size();
			}
			static void write(std::byte*& buffer, uint64_t nextOffset, BytesView value) {
				std::memcpy(buffer, &nextOffset, sizeof(uint64_t));
				buffer += sizeof(uint64_t);
				writeSize(buffer, value.size());
				std::memcpy(buffer, value.data(), value.size());
				buffer += value.size();
			}
			ItemValue next() const {
				return ItemValue(data_, nextOffset());
			}

		private:
			std::byte* itemData() const {
				return data_ + offset_;
			}
		};
		

		class ItemKey {
		private:
			std::byte* data_;
			uint64_t offset_;

			// item layout:
			// nextOffset(8), firstValueOffset(8), keySize(1-8), key(...)

		public:
			ItemKey() : data_(nullptr), offset_(0) {}
			ItemKey(std::byte* data, uint64_t offset)
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
				const std::byte* ptr = itemData() + sizeof(uint64_t) + sizeof(uint64_t);
				auto sKey = readSize(ptr);
				return {(const char*)ptr, sKey};
			}
			ItemValue value() const {
				return ItemValue(data_, valueOffset());
			}
			uint64_t valueOffset() const {
				//return *reinterpret_cast<const uint64_t*>(itemData()+sizeof(uint64_t));
				uint64_t n; std::memcpy(&n, itemData()+sizeof(uint64_t), sizeof(n)); return n;
			}
			void setValueOffset(uint64_t nextOffset) {
				std::byte* dt = itemData()+sizeof(uint64_t);
				std::memcpy(dt, &nextOffset, sizeof(uint64_t));
			}
			size_t calcSize() const {
				const std::byte* ptr = itemData() + sizeof(uint64_t) + sizeof(uint64_t);
				auto sKey = readSize(ptr);
				return ptr - itemData() + sKey;
			}
			static size_t calcSize(std::string_view key) {
				return sizeof(uint64_t) + sizeof(uint64_t) + numBytesSize(key.size()) + key.size();
			}
			static void write(std::byte*& buffer, uint64_t nextOffset, std::string_view key, uint64_t value) {
				std::memcpy(buffer, &nextOffset, sizeof(uint64_t));
				buffer += sizeof(uint64_t);
				std::memcpy(buffer, &value, sizeof(uint64_t));
				buffer += sizeof(uint64_t);
				writeSize(buffer, key.size());
				std::memcpy(buffer, key.data(), key.size());
				buffer += key.size();
			}
			ItemKey next() const {
				return ItemKey(data_, nextOffset());
			}

		private:
			std::byte* itemData() const {
				return data_ + offset_;
			}
		};

	public:
		KeyValueFileList(const fs::path& path);
		~KeyValueFileList();
		KeyValueFileList(const KeyValueFileList&) = delete;
		KeyValueFileList& operator=(const KeyValueFileList&) = delete;
		KeyValueFileList(KeyValueFileList&&) = default;
		KeyValueFileList& operator=(KeyValueFileList&&) = default;

		void set(const std::string& key, const Bytes& value);
		void set(std::string_view key, BytesView value);
		//void setBatch(const std::vector<std::pair<std::string, Bytes>>& arr);
		//void setBatch(const std::vector<std::pair<std::string_view, BytesView>>& arr);
		void set(const KeyValueFileList& db2);
		std::vector<BytesView> get(std::string_view key) const;
		std::vector<BytesView> get(const std::string& key) const;
		std::vector<BytesView> getWithBucket(uint64_t bucket, std::string_view key) const;
		bool exists(std::string_view key, BytesView value) const;
		bool existsWithBucket(uint64_t bucket, std::string_view key, BytesView value) const;
		void remove(std::string_view key, BytesView value);
		void remove(const std::string& key, const Bytes& value);
		void optimize();
		void lockTableForNumKeys(uint64_t n);
		void unlockTable();
		std::vector<std::pair<std::string_view,BytesView>> allDocuments() const;

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

		static uint64_t calcHash(std::string_view key);
		static uint64_t calcBucketFromHash(uint64_t hash, uint64_t numBuckets);
		uint64_t calcBucket(std::string_view key) const;
		//std::vector<uint64_t> calcBuckets(const std::vector<std::pair<std::string_view, BytesView>>& arr) const;
		uint64_t numBuckets() const;
		void ensureOptimalWaste();
		uint64_t wasted() const;
		uint64_t numItems() const;
		uint64_t numKeys() const;
		void clear();

	private:
		void setInternal(uint64_t bucket, std::string_view key, BytesView value);
		void removeInternal(uint64_t bucket, std::string_view key, BytesView value);
		ItemKey firstKey(uint64_t bucket) const;

		
		std::byte* data() const;
		uint64_t tableOffset(uint64_t bucket) const;
		void setTableOffset(uint64_t bucket, uint64_t offset) const;

		
		uint64_t nextDataOffset() const;
		void setNextDataOffset(uint64_t off);
		void setNumItems(uint64_t num);
		void setWasted(uint64_t w);
		void setNumKeys(uint64_t num);

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
