//
//  Db.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <map>
#include <vector>
#include <set>
#include <assert.h> 
#include "Tokenize.hpp"
#include <thread>
#include <fstream>
#include <unordered_set>
#include <chrono>
#include <random>
#include "Types.hpp"
#include "FindMany.hpp"
#include <mutex>


#include <sstream>
namespace tmp {
class osyncstream : public std::stringstream {
private:
	std::ostream& stream_;

public:
	osyncstream(std::ostream& stream)
		: std::stringstream(), stream_(stream)
	{ }
	osyncstream(const osyncstream&) = delete;
	osyncstream& operator=(const osyncstream& other) = delete;
	~osyncstream() {
		stream_ << str();
		stream_ << std::flush;
	}
};
}

namespace Search {

	template<class TStore2>
	class Db
	{
	public:
		typedef TStore2 TStore;
		struct Settings {
			bool autocomplete = true;
			uint8_t autocompleteMaxLen = 0;
		};
		Settings settings;
		
	private:
		TStore& store_;
		mutable std::mutex mutex_;
		
	public:
		Db(TStore& store) : store_(store) {}

		TStore& store() { return store_; }
		const TStore& store() const { return store_; }

		void add(const typename TStore::TDoc& doc)
		{
			// lock with mutex
			std::lock_guard<std::mutex> lock(mutex_);

			auto id = doc.docId();
			
			// all tokens
			auto resTokens = documentTokens(doc);
			std::unordered_set<std::string>& tokensAdd = std::get<0>(resTokens);
			std::vector<std::string>& tokensJoined = std::get<1>(resTokens);

			std::vector<std::string> arrOld;
			auto res123 = store_.findDoc(id);
			if( res123 ) {
				arrOld = res123->second;
			}
			
			// combine
			std::unordered_set<std::string> tokensRemove;
			for( const auto& txt : arrOld ) {
				auto tks = splitTokens(txt);
				tokensRemove.insert(tks.begin(), tks.end());
			}
			// difference
			tokensDifference(tokensAdd, tokensRemove);
			
			auto tokensAddPartial = partialTokens(tokensAdd);
			auto tokensRemovePartial = partialTokens(tokensRemove);
			tokensDifference(tokensAddPartial, tokensRemovePartial);

			for( const auto& tk : tokensRemove ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = true;
				store_.removeToken(std::string(tk), ti);
			}
			for( const auto& tk : tokensRemovePartial ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = false;
				store_.removeToken(std::string(tk), ti);
			}
			for( const auto& tk : tokensAdd ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = true;
				store_.addToken(std::string(tk), ti);
			}
			for( const auto& tk : tokensAddPartial ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = false;
				store_.addToken(std::string(tk), ti);
			}
			store_.addDoc(id, doc, tokensJoined);
		}
		
		void remove(const typename TStore::TDoc::TId& id)
		{
			// lock with mutex
			std::lock_guard<std::mutex> lock(mutex_);

			auto opt = store_.findDoc(id);
			if( !opt )
				return;
			auto& pair = *opt;
			auto& doc = pair.first;
			auto& tokensJoinedArr = pair.second;

			std::unordered_set<std::string> tokensRemove;
			for( const auto& txt : tokensJoinedArr ) {
				auto tks = splitTokens(txt);
				tokensRemove.insert(tks.begin(), tks.end());
			}
			
			auto tokensRemovePartial = partialTokens(tokensRemove);
			for( const auto& tk : tokensRemove ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = true;
				store_.removeToken(std::string(tk), ti);
			}
			for( const auto& tk : tokensRemovePartial ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = false;
				store_.removeToken(std::string(tk), ti);
			}
			store_.removeDoc(id);
		}
		
		std::unordered_set<typename TStore::TDoc::TId> findMatchAll(const SearchSettings<typename TStore::TDoc>& searchSett) const
		{
			// lock with mutex
			std::lock_guard<std::mutex> lock(mutex_);

			std::unordered_set<typename TStore::TDoc::TId> all;
			for( size_t i=0; i<searchSett.tokens.size(); ++i ) {
				bool isPartial = searchSett.autocomplete && i+1 == searchSett.tokens.size() && settings.autocomplete;
				std::string token = searchSett.tokens[i];
				if(isPartial && token.size() == 1 )
					continue;

				if(isPartial) {
					if( token.size() > settings.autocompleteMaxLen && settings.autocompleteMaxLen > 0 ) {
						size_t newSize = 0;
						while(true) {
							auto l = charLen(token[newSize]);
							if( newSize + l > settings.autocompleteMaxLen )
								break;
							newSize += l;
						}
						token.resize(newSize);
					}
				}

				auto vec = store_.findToken(token);
				
				std::unordered_set<typename TStore::TDoc::TId> allIds;

				if(isPartial) {
					if(token.size() != searchSett.tokens[i].size()) {
						// delete documents that do not contain full phrase
						token = searchSett.tokens[i];
						vec.erase(
							std::remove_if(
								vec.begin(),
								vec.end(),
								[=](const typename TStore::TTokenInfo& mtc){
									auto opt = store().findDoc(mtc.docId);
									if(!opt)
										return true;
									auto& pair = *opt;
									auto& tokens = pair.second;
									
									auto tokensJoined = joinTokens(tokens);
									if(tokensJoined.empty())
										return true;
									size_t pos = 0;
									while(true) {
										pos = tokensJoined.find(token, pos);
										if( pos == std::string::npos ) {
											return true;
										}
										if( pos == 0 ) {
											if( tokensJoined.size() == token.size() ) {
												return false;
											}
											if( tokensJoined[token.size()] == ' ' ) {
												return false;
											}
											pos += 1;
											continue;
										}
										if( tokensJoined[pos-1] == ' ' ) {
											return false;
										}
										pos += 1;
										continue;
									}
								}
							),
							vec.end()
						);
					}
					// keep one id
					for(auto& tki : vec) {
						allIds.insert(tki.docId);
					}
				}
				else {
					// remove non whole
					for( const auto& tki : vec ) {
						if( tki.isWhole ) {
							allIds.insert(tki.docId);
						}
					}
				}

				if( allIds.empty() && !searchSett.matchAnyToken )
					return {};
				
				if(!searchSett.matchAnyToken)
				{
					if( i == 0 ) {
						// copy
						all = allIds;
					}
					else {
						// remove from all, which isnt in vec
						std::unordered_set<typename TStore::TDoc::TId> all2;
						for( const auto& id : all ) {
							auto ptr = allIds.find(id);
							if(ptr != allIds.end())
								all2.insert(id);
						}
						all = all2;
					}
				}
				else
				{
					all.insert(allIds.begin(), allIds.end());
				}
			}
			return all;
		}

private:
		struct BulkThreadRes
		{
			size_t numDocs;
			fs::path pathTokens;
			fs::path pathDocs;
			std::unordered_set<std::string> tokens;
		};

		void bulkAddThreadDocs(size_t nthThread, size_t numThreads, const std::vector<std::pair<const std::byte*, const std::byte*>>& datas)
		{
			for( size_t i=0; i<numThreads; ++i )
			{
				size_t n = 0;
				const std::byte* dt = datas[i].first;
				while(dt < datas[i].second) {
					store_.bulkDocsRead(dt, nthThread, numThreads);
					n++;
				}
			}
		}

		void bulkAddThreadTokens(size_t nthThread, size_t numThreads, const std::vector<std::pair<const std::byte*, const std::byte*>>& datas)
		{
			for( size_t i=0; i<numThreads; ++i )
			{
				const std::byte* dt = datas[i].first;
				while(dt < datas[i].second) {
					store_.bulkTokensReadAdd(dt, nthThread, numThreads);
					store_.bulkTokensReadRemove(dt, nthThread, numThreads);
				}
			}
		}

	public:
		class BulkWriter
		{
			friend class Db;
			
		private:
			Db<TStore>& db_;
			size_t numDocs;
			fs::path pathTokens;
			fs::path pathDocs;
			std::unordered_set<std::string> tokens;
			std::ofstream oDocs;
			std::ofstream oTokens;

		public:
			BulkWriter(Db<TStore>& db)
				: db_(db), numDocs(0)
			{
				// open file
				pathDocs = tmp_unique_path();
				oDocs.open(pathDocs.string(), std::ofstream::binary);
				if( !oDocs.is_open() ) {
					std::cout << pathDocs.string() << std::endl;
					throw std::runtime_error("oDocs Cant open file");
				}
				pathTokens = tmp_unique_path();
				oTokens.open(pathTokens.string(), std::ofstream::binary);
				if( !oTokens.is_open() ) {
					std::cout << pathTokens.string() << std::endl;
					throw std::runtime_error("oTokens Cant open file");
				}
			}
			BulkWriter(const BulkWriter&) = delete;
			BulkWriter& operator=(const BulkWriter&) = delete;
			BulkWriter(BulkWriter&&) = default;
			BulkWriter& operator=(BulkWriter&&) = default;

			void close()
			{
				oDocs.close();
				oTokens.close();
			}

			void add(const typename TStore::TDoc& doc)
			{
				auto id = doc.docId();
				numDocs += 1;

				// prepare tokens
				auto resTokens = documentTokens(doc);
				std::unordered_set<std::string> tokensAdd = std::get<0>(resTokens);
				std::vector<std::string> tokensJoined = std::get<1>(resTokens);

				// check what to remove
				std::unordered_set<std::string> tokensRemove;
				auto res123 = db_.store().findDoc(doc.docId());
				if( res123 ) {
					for( const auto& txt : res123->second ) {
						auto tks = splitTokens(txt);
						tokensRemove.insert(tks.begin(), tks.end());
					}
					tokensDifference(tokensAdd, tokensRemove);
				}

				// write to file DOC
				TStore::bulkDocWrite(oDocs, id, doc, tokensJoined);

				auto tokensAddPartial = db_.partialTokens(tokensAdd);
				auto tokensRemovePartial = db_.partialTokens(tokensRemove);
				tokensDifference(tokensAddPartial, tokensRemovePartial);

				// write to file Tokens
				// add
				{
					std::vector<std::pair<std::string, typename TStore::TTokenInfo>> arr;
					for( const auto& tk : tokensAdd ) {
						typename TStore::TTokenInfo ti;
						ti.docId = id;
						ti.isWhole = true;
						arr.push_back({std::string(tk), ti});
					}
					for( const auto& tk : tokensAddPartial ) {
						typename TStore::TTokenInfo ti;
						ti.docId = id;
						ti.isWhole = false;
						arr.push_back({std::string(tk), ti});
					}
					TStore::bulkTokensWrite(oTokens, arr);
				}
				// remove
				{
					std::vector<std::pair<std::string, typename TStore::TTokenInfo>> arr;
					for( const auto& tk : tokensRemove ) {
						typename TStore::TTokenInfo ti;
						ti.docId = id;
						ti.isWhole = true;
						arr.push_back({std::string(tk), ti});
					}
					for( const auto& tk : tokensRemovePartial ) {
						typename TStore::TTokenInfo ti;
						ti.docId = id;
						ti.isWhole = false;
						arr.push_back({std::string(tk), ti});
					}
					TStore::bulkTokensWrite(oTokens, arr);
				}

				// combine all tokens
				tokens.insert(tokensAdd.begin(), tokensAdd.end());
			}
		};

		std::vector<BulkWriter> bulkWriters(size_t numThreads)
		{
			std::vector<BulkWriter> arr;
			arr.reserve(numThreads);
			for( size_t i=0; i<numThreads; ++i ) {
				arr.emplace_back(*this);
			}
			return arr;
		}

		void bulkAdd(std::vector<BulkWriter>& writers)
		{
			auto numThreads = writers.size();
			auto t1 = std::chrono::high_resolution_clock::now();

			// close files
			for(auto& w : writers) {
				w.close();
			}

			// calc num docs and tokens
			size_t numDocs = 0;
			size_t numTokens;
			{
				std::unordered_set<std::string_view> allTokens;
				for(auto& w : writers) {
					numDocs += w.numDocs;
					allTokens.insert(w.tokens.begin(), w.tokens.end());
					auto partial = partialTokens(w.tokens, &allTokens);
					allTokens.insert(partial.begin(), partial.end());
				}
				numTokens = allTokens.size();
				for(auto& w : writers) {
					w.tokens.clear();
				}
			}
			
			auto t2 = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double> d2 = t2 - t1;
			{ tmp::osyncstream(std::cout) << "Counted " << numTokens << " tokens in " << d2.count() << " sec\n"; }

			// lock with mutex
			std::lock_guard<std::mutex> lock(mutex_);
			
			std::vector<std::thread> pool;
			std::vector<boost::iostreams::mapped_file> files;
			std::vector<std::pair<const std::byte*, const std::byte*>> filesData;
			store_.bulkStart(numThreads);

			// - - - - - - - - - - - - - - 
			// - - - - INSERT DOCS - - - -
			// - - - - - - - - - - - - - - 
			files.resize(numThreads);
			filesData.resize(numThreads);
			for( size_t i=0; i<numThreads; ++i )
			{
				if(fs::file_size(writers[i].pathDocs) == 0) {
					filesData[i] = {nullptr, nullptr};
					continue;
				}
				//if(fs::file_size)
				auto& file = files[i];
				file.open(writers[i].pathDocs);
				if( !file.is_open() )
					throw std::runtime_error("INSERT DOCS Cant open file");
				assert(file.data());
				filesData[i] = {(const std::byte*)file.data(), (const std::byte*)file.data()+file.size()};
			}
			store_.bulkDocsLock(store_.sizeDocuments() + numDocs);
			
			// + + + +
			for( size_t i=0; i<numThreads; ++i ) {
				pool.emplace_back(&Db::bulkAddThreadDocs, this, i, numThreads, filesData);
			}
			for(auto& th : pool) {
				th.join();
			}
			pool.clear();
			// + + + +

			store_.bulkDocsUnlock();
			//for( auto& file : files ) {
			for( size_t i=0; i<numThreads; ++i ) {
				if(fs::file_size(writers[i].pathDocs) == 0) {
					continue;
				}
				files[i].close();
			}
			files.clear();
			filesData.clear();
			auto t3 = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double> d3 = t3 - t2;
			{ tmp::osyncstream(std::cout) << "Insert docs finished in " << d3.count() << " sec\n"; }


			// - - - - - - - - - - - - - - 
			// - - -  INSERT TOKENS  - - -
			// - - - - - - - - - - - - - - 
			files.resize(numThreads);
			filesData.resize(numThreads);
			for( size_t i=0; i<numThreads; ++i )
			{
				if(fs::file_size(writers[i].pathTokens) == 0) {
					continue;
				}
				auto& file = files[i];
				file.open(writers[i].pathTokens);
				if( !file.is_open() )
					throw std::runtime_error("INSERT TOKENS Cant open file");
				assert(file.data());
				filesData[i] = {(const std::byte*)file.data(), (const std::byte*)file.data()+file.size()};
			}
			store_.bulkTokensLock(store_.sizeTokens()+numTokens);
			
			// + + + +
			for( size_t i=0; i<numThreads; ++i ) {
				pool.emplace_back(&Db::bulkAddThreadTokens, this, i, numThreads, filesData);
			}
			for(auto& th : pool) {
				th.join();
			}
			pool.clear();
			// + + + +
			
			store_.bulkTokensUnlock();
			for( size_t i=0; i<numThreads; ++i ) {
				if(fs::file_size(writers[i].pathTokens) == 0) {
					continue;
				}
				files[i].close();
			}
			files.clear();
			filesData.clear();
			auto t4 = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double> d4 = t4 - t3;
			{ tmp::osyncstream(std::cout) << "Insert tokens finished in " << d4.count() << " sec\n"; }
			
			
			
			// - - - - - - - - - - - - - - 
			// - - - - - - CLEAN - - - - -
			// - - - - - - - - - - - - - - 
			store_.bulkStop();
			for( size_t i=0; i<numThreads; ++i ) {
				fs::remove(writers[i].pathDocs);
				fs::remove(writers[i].pathTokens);
			}
			writers.clear();
			std::cout << "Cleaned\n";
			
			
			
			// - - - - - - - - - - - - - -
			// - - - - - OPTIMIZE - - - -
			// - - - - - - - - - - - - - -
			store_.optimizeFreeData();
			auto t5 = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double> d5 = t5 - t4;
			{ tmp::osyncstream(std::cout) << "OPTIMIZED in " << d5.count() << " sec\n"; }
		}
		
	private:
		inline static std::tuple<std::unordered_set<std::string>, std::vector<std::string> /*, std::string*/> documentTokens(const typename TStore::TDoc& doc)
		{
			auto txts = doc.allTexts();
			std::unordered_set<std::string> tokensFull;
			std::vector<std::string> tokensJoined;
			tokensJoined.reserve(txts.size());
			for( const auto& txt : txts ) {
				auto tks = tokenize(txt);
				tokensFull.insert(tks.begin(), tks.end());
				auto tmp = joinTokens(tks);
				tokensJoined.push_back(tmp);
			}
			return {tokensFull, tokensJoined /*, joinTokens(tokensJoined)*/};
		}
		std::unordered_set<std::string_view> partialTokens(const std::unordered_set<std::string>& tokens, std::unordered_set<std::string_view>* allTokens=nullptr)
		{
			if(!settings.autocomplete)
				return {};

			std::unordered_set<std::string_view> tks;
			for( const auto& token : tokens ) {
				for( size_t i=0; i<token.size(); ) {
					auto l = charLen(token[i]);
					if( i+l >= token.size() ) {
						break;
					}
					if(i+1 >= 2) {
						if(settings.autocompleteMaxLen==0 || i+l <= settings.autocompleteMaxLen ) {
							if(allTokens) {
								allTokens->insert({token.data(), i+l});
							}
							else {
								tks.insert({token.data(), i+l});
							}
						}
					}
					i += l;
				}
			}
			return tks;
		}
		/*std::vector<std::string_view> allTokenVersions(const std::string& token)
		{
			std::vector<std::string_view> all;
			all.reserve(std::min(token.size(),(size_t)10));
			for( size_t i=0; i<token.size(); ) {
				auto l = charLen(token[i]);
				if( (settings.autocomplete && settings.autocompleteMaxLen==0) || (settings.autocomplete && i+1 >= 2 && i+l <= settings.autocompleteMaxLen) || i+l == token.size() ) {
					all.push_back(std::string_view(token.data(), i+l));
				}
				i += l;
			}
			return all;
		}*/
		/*void addToken(const std::string& token, const typename TStore::TDoc::TId& id)
		{
			auto all = allTokenVersions(token);
			for( const auto& tk : all ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = token.size() == tk.size();
				store_.addToken(std::string(tk), ti);
			}
		}
		
		void removeToken(const std::string& token, const typename TStore::TDoc::TId& id)
		{
			auto all = allTokenVersions(token);
			for( const auto& tk : all ) {
				typename TStore::TTokenInfo ti;
				ti.docId = id;
				ti.isWhole = token.size() == tk.size();
				store_.removeToken(std::string(tk), ti);
			}
		}*/

		template <class T>
		static void tokensDifference(std::unordered_set<T>& tokensAdd, std::unordered_set<T>& tokensRemove)
		{
			if( tokensAdd.size() < tokensRemove.size() ) {
				std::unordered_set<T> tmp = tokensAdd;
				for( const auto& token : tmp ) {
					auto ptrRem = tokensRemove.find(token);
					if( ptrRem != tokensRemove.end() ) {
						tokensRemove.erase(ptrRem);
						auto ptrAdd = tokensAdd.find(token);
						tokensAdd.erase(ptrAdd);
					}
				}
			}
			else {
				std::unordered_set<T> tmp = tokensRemove;
				for( const auto& token : tmp ) {
					auto ptrAdd = tokensAdd.find(token);
					if( ptrAdd != tokensAdd.end() ) {
						tokensAdd.erase(ptrAdd);
						auto ptrRem = tokensRemove.find(token);
						tokensRemove.erase(ptrRem);
					}
				}
			}
		}
		
		static fs::path tmp_unique_path()
		{
			thread_local static std::random_device rd;
			static thread_local std::mt19937 rng(rd());
			std::uniform_int_distribution<std::mt19937::result_type> dist(100000000,999999999);
			
			auto folder = fs::temp_directory_path();
			while(true)
			{
				auto pth = folder;
				auto n = dist(rng);
				pth /= "seach-" + std::to_string(n);
				if( !fs::exists(pth) ) {
					return pth;
				}
			}
		}
	};
}
