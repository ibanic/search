//
//  SearchSettings_hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <vector>
#include <string>
#include <exception>

namespace Search {
	template <class TDoc2>
	class Result {
	public:
		typedef TDoc2 TDoc;

		size_t dbIndex;
		typename TDoc::TId id;

		size_t index;
		TDoc doc;
		std::vector<std::string> tokens;

		Result() = default;
		Result(size_t dbIndex2, typename TDoc::TId id2, size_t index2, const TDoc& doc2, const std::vector<std::string>& tokens2)
			: dbIndex(dbIndex2), id(id2), index(index2), doc(doc2), tokens(tokens2)
		{ }
	};


	struct SearchManager {
	private:
		//std::atomic<bool> continueSearch;
		std::atomic_flag continueSearch = ATOMIC_FLAG_INIT;  // until C++20 init by hand

	public:
		SearchManager() {
			reset();
		}

		inline void cancel() {
			continueSearch.clear();
			//continueSearch = false;
		}

		inline bool shouldContinue() {
			auto res = continueSearch.test_and_set();
			return res;
			//return continueSearch;
		}

		inline void reset() {
			continueSearch.test_and_set();
			//continueSearch = true;
		}
	};

	template <class TDoc>
	struct SearchSettings
	{
	public:
		std::string query;
		std::vector<std::string> tokens;
		std::string tokensJoined;
		bool autocomplete = true;
		std::function<bool(const Result<TDoc>&)> funcFilter;
		bool matchAnyToken = false;
		SearchManager* manager = nullptr;
	};


	class SearchCanceledException : public std::exception {
	public:
		virtual const char* what() const noexcept {
			return "search was canceled";
		}
	};
}
