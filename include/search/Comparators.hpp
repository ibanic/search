//
//  Comparators.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <map>
#include <vector>
#include <search/Tokenize.hpp>
#include <search/SearchSettings.hpp>


namespace Search {

	

	template <class TRes>
	class CompIsWhole {
	private:
		const std::vector<std::string>* tokens_;
		mutable std::vector<uint8_t> cache;

		bool calc(const std::vector<std::string>& tks) const {
			auto tks2 = joinTokens(tks);
			for( const auto& tk : *tokens_ ) {
				if( !tokensOverlap(tks2, tk) ) {
					return false;
				}
			}
			return true;
		}

	public:
		CompIsWhole() = default;

		void init(const std::vector<TRes>& all, const SearchSettings<typename TRes::TDoc>& sett) {
			tokens_ = &sett.tokens;
			cache = std::vector<uint8_t>(all.size(), 0);
		}

		void clean() { }

		int compare(const TRes& d1, const TRes& d2) const {
			if( !cache[d1.index] ) {
				cache[d1.index] = calc(d1.tokens) ? 10 : 8;
			}
			if( !cache[d2.index] ) {
				cache[d2.index] = calc(d2.tokens) ? 10 : 8;
			}
			auto h1 = cache[d1.index] == 10;
			auto h2 = cache[d2.index] == 10;
			
			if( h1 && !h2 )
				return -1;
			if( !h1 && h2 )
				return 1;
			return 0;
		}
	};

	template <class TRes>
	class CompWordsTogether {
	private:
		mutable std::vector<uint8_t> cache;
		std::string tokensSearch_;
		std::vector<size_t> lens_;

	public:
		CompWordsTogether() = default;

		void init(const std::vector<TRes>& all, const SearchSettings<typename TRes::TDoc>& sett) {
			cache = std::vector<uint8_t>(all.size(), 0);
			tokensSearch_ = joinTokens(sett.tokens);
			lens_.clear();
			// calc lens
			size_t l = 0;
			for( const auto& tk : sett.tokens ) {
				if(l != 0)
					l++;
				l += tk.size();
				lens_.push_back(l);
				if( lens_.size()+1 == std::numeric_limits<uint8_t>::max() )  // leave last free, later add one to indicate value is calculated
					break;
			}
		}

		void clean() { }

		uint8_t calc(const std::vector<std::string>& tokensAll2) const {
			auto tokensAll = joinTokens(tokensAll2);
			for( uint8_t i=lens_.size(); i>0; --i ) {
				std::string_view tks2(tokensSearch_.data(), lens_[i-1]);
				if( tokensOverlap(tokensAll, tks2) ) {
					return i;
				}
			}
			return 0;
		}

		int compare(const TRes& d1, const TRes& d2) const {
			if( !cache[d1.index] ) {
				cache[d1.index] = calc(d1.tokens)+1;
			}
			if( !cache[d2.index] ) {
				cache[d2.index] = calc(d2.tokens)+1;
			}
			auto n1 = cache[d1.index]-1;
			auto n2 = cache[d2.index]-1;

			if( n1 > n2 )
				return -1;
			if( n2 > n1 )
				return 1;
			return 0;
		}
	};


	// comparator that requires function for callback
	// callback should update array with text priorities
	template <class TRes>
	class CompPriorityTextsCallback {
	private:
		const SearchSettings<typename TRes::TDoc>* sett_;
		mutable std::vector<uint8_t> cache_;
		std::function<void(const typename TRes::TDoc, std::vector<uint8_t>&)> cb_;

	public:
		CompPriorityTextsCallback(std::function<void(const typename TRes::TDoc&,std::vector<uint8_t>&)> cb) : cb_(cb) { }

		void init(const std::vector<TRes>& all, const SearchSettings<typename TRes::TDoc>& sett) {
			sett_ = &sett;
			cache_ = std::vector<uint8_t>(all.size(), 0);
		}

		void clean() { }

		int compare(const TRes& d1, const TRes& d2) const {
			if(!cache_[d1.index]) {
				cache_[d1.index] = calc(d1.doc, d1.tokens)+1;
			}
			if(!cache_[d2.index]) {
				cache_[d2.index] = calc(d2.doc, d2.tokens)+1;
			}
			
			auto n1 = cache_[d1.index]-1;
			auto n2 = cache_[d2.index]-1;

			if( n1 > n2 )
				return -1;
			if( n2 > n1 )
				return 1;
			return 0;
		}
		
		uint8_t calc(const typename TRes::TDoc& doc, const std::vector<std::string>& tokens) const
		{
			std::vector<uint8_t> priorities(tokens.size(), 0);
			cb_(doc, priorities);
			uint8_t max_priority = 0;
			for( size_t i=0; i<tokens.size(); ++i ) {
				if( priorities[i] <= max_priority )
					continue;
				if( tokensOverlap(tokens[i], sett_->tokensJoined) ) {
					max_priority = priorities[i];
				}
			}
			return max_priority;
		}
	};
	/*template <class TRes>
	class CompPriorityTextsCallback {
	private:
		const std::vector<std::string>* tokens_;
		mutable std::vector<uint8_t> cache_;
		std::function<std::vector<size_t>(const typename TRes::TDoc)> cb_;

	public:
		CompPriorityTextsCallback(std::function<std::vector<size_t>(const typename TRes::TDoc&)> cb) : cb_(cb) { }

		void init(const std::vector<TRes>& all, const SearchSettings<typename TRes::TDoc>& sett) {
			tokens_ = &sett.tokens;
			cache_ = std::vector<uint8_t>(all.size(), 0);
		}

		void clean() { }

		int compare(const TRes& d1, const TRes& d2) const {
			if(!cache_[d1.index]) {
				cache_[d1.index] = calc(d1.doc, d1.tokens)+1;
			}
			if(!cache_[d2.index]) {
				cache_[d2.index] = calc(d2.doc, d2.tokens)+1;
			}
			
			auto n1 = cache_[d1.index]-1;
			auto n2 = cache_[d2.index]-1;

			if( n1 > n2 )
				return -1;
			if( n2 > n1 )
				return 1;
			return 0;
		}
		
		uint8_t calc(const typename TRes::TDoc& doc, const std::vector<std::string>& tokens) const
		{
			auto arr1 = cb_(doc);
			if(arr1.empty())
				return 0;
			
			size_t num = 0;
			for(const std::string& tk : *tokens_ ) {
				bool found = false;
				for( size_t idx : arr1 ) {
					bool ov = tokensOverlap(tokens[idx], tk);
					if( ov ) {
						found = true;
						break;
					}
				}
				if(found) {
					num += 1;
				}
			}
			
			if(num < std::numeric_limits<uint8_t>::max())
				return static_cast<uint8_t>(num);
			return std::numeric_limits<uint8_t>::max()-1;
		}
	};*/




}
