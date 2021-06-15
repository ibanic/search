//
//  FileStore.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//


#pragma once

#include <string>
#include <map>
#include <vector>
#include <optional>
#include <search/TokenInfo.hpp>
#include <filesystem>
#include <search/KeyValueFile.hpp>
#include <search/KeyValueFileList.hpp>
#include <search/KeyValueMemory.hpp>
#include <search/CompressSize.hpp>
#include <search/Types.hpp>
#include <cstring>


namespace fs = std::filesystem;

namespace Search {

	template<class TDoc2>
	class FileStore {
	public:
		typedef TDoc2 TDoc;
		typedef TokenInfo<typename TDoc::TId> TTokenInfo;
		
	private:
		fs::path path_;
		fs::path path1_;
		fs::path path2_;
		KeyValueFile db;
		KeyValueFileList db2;
		uint64_t numBucketsImport1_, numBucketsImport2_;

	public:
		FileStore(const fs::path& path)
		: path_(path), path1_(path.string()+".docs"), path2_(path.string()+".tokens"), db(path1_), db2(path2_)
		{
		}
		//~FileStore() = default;
		FileStore(const FileStore&) = delete;
		FileStore& operator=(const FileStore&) = delete;
		FileStore(FileStore&&) = default;
		FileStore& operator=(FileStore&&) = default;
		
		static bool isFileVersionOk(const fs::path& path)
		{
			return KeyValueFile::isFileVersionOk(path.string()+".docs") && KeyValueFileList::isFileVersionOk(path.string()+".tokens");
		}

		void addDoc(const typename TDoc::TId& id2, const TDoc& doc, const std::vector<std::string>& tokens)
		{
			auto id = TDoc::serializeId(id2);
			auto cmb = docSerialize(doc, tokens);
			db.set(std::string_view((const char*)&id[0], sizeof(id)), {cmb.data(), cmb.size()});
		}

		void removeDoc(const typename TDoc::TId& id)
		{
			auto key2 = TDoc::serializeId(id);
			db.remove(std::string_view((const char*)&key2[0], sizeof(key2)));
		}

		std::optional<std::pair<TDoc,std::vector<std::string>>> findDoc(const typename TDoc::TId& id) const
		{
			std::string value;
			auto key2 = TDoc::serializeId(id);
			BytesView res;
			res = db.get(std::string_view((const char*)&key2[0], sizeof(key2)));

			if(!res.data())
				return std::nullopt;

			return docDeserialize(id, res);
		}

		std::vector<TDoc> allDocuments() const {
			auto arr2 = db.allDocuments();
			std::vector<TDoc> arr;
			arr.reserve(arr2.size());
			for( const auto& pair : arr2 ) {
				typename TDoc::TIdSerialized id2;
				std::memcpy(&id2[0], pair.first.data(), pair.first.size());
				auto id = TDoc::deserializeId(id2);
				auto res = docDeserialize(id, pair.second);
				arr.push_back(res.first);
			}
			return arr;
		}
		
		void addToken(std::string_view token, const TTokenInfo& info)
		{
			db2.set(token, tokenInfoToString(info));
		}
		
		void removeToken(std::string_view token, const TTokenInfo& info)
		{
			db2.remove(token, tokenInfoToString(info));
		}
		
		std::vector<TTokenInfo> findToken(const std::string& token)
		{
			auto res = db2.get(token);
			std::vector<TTokenInfo> arr(res.size());
			for(size_t i=0; i<res.size(); ++i) {
				arr[i] = tokenInfoFromString(res[i]);
			}
			return arr;
		}


		void optimize() {
			db.optimize();
			db2.optimize();
		}


		void optimizeFreeData() {
			db.ensureOptimalWaste();
			db2.ensureOptimalWaste();
		}

		size_t fileSize() const
		{
			return db.fileSize() + db2.fileSize();
		}
		
		const fs::path& path() const { return path_; }


		static void removeFiles(const fs::path& pth2)
		{
			fs::path pth3;
			pth3 = pth2;
			pth3 += ".docs";
			if( fs::is_regular_file(pth3) ) {
				fs::remove(pth3);
			}
			pth3 = pth2;
			pth3 += ".tokens";
			if( fs::is_regular_file(pth3) ) {
				fs::remove(pth3);
			}
		}

		void clear() {
			db.clear();
			db2.clear();
		}

		size_t sizeDocuments() {
			return db.numItems();
		}

		size_t sizeTokens() {
			return db2.numItems();
		}


		void bulkStart(size_t numThreads)
		{
			db.bulkStart(numThreads);
			db2.bulkStart(numThreads);
		}
		void bulkStop()
		{
			db.bulkStop();
			db2.bulkStop();
		}
		static void bulkDocWrite(std::ofstream& out, const typename TDoc::TId& id2, const TDoc& doc, const std::vector<std::string>& tokens)
		{
			auto id_s = TDoc::serializeId(id2);
			std::string_view id_view((const char*)&id_s[0], sizeof(id_s));
			auto cmb = docSerialize(doc, tokens);
			KeyValueFile::bulkWrite(out, id_view, BytesView(cmb.data(), cmb.size()));
		}
		void bulkDocsRead(const std::byte*& dt, size_t nthThread, size_t numThreads)
		{
			auto r1 = db.bulkRead(dt);

			auto hash = std::get<0>(r1);
			auto bucket = db.calcBucketFromHash(hash, numBucketsImport1_);
			if( !db.bulkIsInThread(bucket, nthThread, numThreads, numBucketsImport1_) ) {
				return;
			}

			db.bulkInsert(bucket, std::get<1>(r1), std::get<2>(r1), nthThread, numThreads);
		}
		void bulkTokensReadAdd(const std::byte*& dt, size_t nthThread, size_t numThreads)
		{
			auto len = readSize(dt);
			for( size_t i=0; i<len; ++i ) {
				auto r1 = db2.bulkRead(dt);
				auto hash = std::get<0>(r1);
				auto bucket = db2.calcBucketFromHash(hash, numBucketsImport2_);
				if( !db2.bulkIsInThread(bucket, nthThread, numThreads, numBucketsImport2_) ) {
					continue;
				}
				
				assert(std::get<1>(r1).size() > 0);

				db2.bulkInsert(bucket, std::get<1>(r1), std::get<2>(r1), nthThread, numThreads);
			}
		}
		void bulkTokensReadRemove(const std::byte*& dt, size_t nthThread, size_t numThreads)
		{
			auto len = readSize(dt);
			for( size_t i=0; i<len; ++i ) {
				auto r1 = db2.bulkRead(dt);
				auto hash = std::get<0>(r1);
				auto bucket = db2.calcBucketFromHash(hash, numBucketsImport2_);
				if( !db2.bulkIsInThread(bucket, nthThread, numThreads, numBucketsImport2_) ) {
					continue;
				}

				db2.bulkRemove(bucket, std::get<1>(r1), std::get<2>(r1), nthThread, numThreads);
			}
		}
		static void bulkTokensWrite(std::ofstream& out, const std::vector<std::pair<std::string, TTokenInfo>>& arr)
		{
			auto buff = writeSizeString(arr.size());
			out.write((const char*)buff.data(), buff.size());
			for( const auto& pair : arr ) {
				KeyValueFileList::bulkWrite(out, pair.first, tokenInfoToString(pair.second));
			}
		}
		void bulkTokensLock(size_t numItems)
		{
			db2.lockTableForNumKeys(numItems);
			numBucketsImport2_ = db2.numBuckets();
		}
		void bulkTokensUnlock()
		{
			db2.unlockTable();
			numBucketsImport2_ = 0;
		}
		void bulkDocsLock(size_t numItems)
		{
			db.lockTableForNumItems(numItems);
			numBucketsImport1_ = db.numBuckets();
		}
		void bulkDocsUnlock()
		{
			db.unlockTable();
			numBucketsImport1_ = 0;
		}

		
	private:
		static Bytes docSerialize(const TDoc& doc, const std::vector<std::string>& tokens)
		{
			auto val = doc.serialize();
			auto tokens2 = docTocsSerialize(tokens);

			Bytes cmb(numBytesSize(val.size())+val.size()+tokens2.size(), (std::byte)'\0');
			std::byte* dt = &cmb[0];
			writeSize(dt, val.size());
			std::memcpy(dt, val.data(), val.size());
			dt += val.size();
			std::memcpy(dt, tokens2.data(), tokens2.size());

			return cmb;
		}

		static std::pair<TDoc, std::vector<std::string>> docDeserialize(const typename TDoc::TId& id, BytesView txt)
		{
			auto dt = txt.data();
			auto l = readSize(dt);
			auto sizeLen = dt - txt.data();
			TDoc doc(id, BytesView(dt, l));
			auto vec = docTocsDeserialize(BytesView(dt+l, txt.size()-sizeLen-l));
			return {doc, vec};
		}
		
		static TTokenInfo tokenInfoFromString(BytesView txt)
		{
			TTokenInfo info;
			typename TDoc::TIdSerialized tmp;
			std::memcpy(&tmp[0], txt.data(), sizeof(tmp));
			info.docId = TDoc::deserializeId(tmp);
			info.isWhole = txt[sizeof(tmp)] == (std::byte)'1';
			return info;
		}
		
		static Bytes tokenInfoToString(const TTokenInfo& info)
		{
			Bytes txt(sizeof(typename TDoc::TIdSerialized)+1, (std::byte)'\0');
			std::byte* cur = &txt[0];
			auto tmp = TDoc::serializeId(info.docId);
			std::memcpy(txt.data(), &tmp[0], sizeof(tmp));
			txt[sizeof(tmp)] = info.isWhole ? (std::byte)'1' : (std::byte)'0';
			return txt;
		}

		static Bytes docTocsSerialize(const std::vector<std::string>& arr)
		{
			if(arr.empty())
				return {};

			size_t len = 0;
			for( const auto& txt : arr ) {
				len += numBytesSize(txt.size());
				len += txt.size();
			}

			Bytes dt(len, (std::byte)'\0');
			std::byte* cur = &dt[0];
			for( const auto& txt : arr ) {
				writeSize(cur, txt.size());
				std::memcpy(cur, txt.data(), txt.size());
				cur += txt.size();
			}
			return dt;
		}

		static std::vector<std::string> docTocsDeserialize(BytesView dt)
		{
			if(dt.size() == 0) {
				return {};
			}

			const std::byte* cur = dt.data();
			const std::byte* end = cur + dt.size();
			std::vector<std::string> arr;
			while(cur < end) {
				auto l = readSize(cur);
				arr.emplace_back((const char*)cur, l);
				cur += l;
			}
			if(cur > end) {
				throw std::runtime_error("docTocsDeserialize() overflow");
			}
			return arr;
		}
	};


} 
