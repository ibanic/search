//
//  FindMany.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include "Sort.hpp"
#include "Tokenize.hpp"
#include "SearchSettings.hpp"

namespace Search {
	/*template <class DbType, class TDoc>
	class Result {
	public:
		const DbType* db;
		TDoc doc;
		std::vector<ExcerptPart> excerpt;
		Result(const DbType* db2, TDoc doc2) : db(db2), doc(doc2) {}
	};*/

	template <class DbType, class ... Ts >  // template<typename> typename... Rest
	std::vector<Result<typename DbType::TStore::TDoc> > findMany(const std::vector<const DbType*>& dbs, SearchSettings<typename DbType::TStore::TDoc>& sett, Ts&... cmps)
	{
		// Tokenize: ?
		// Load tokens: 62 %
		// Load docs: 28 %
		// Sort: 10 %
		
		sett.tokens = tokenize(sett.query);
		sett.tokensJoined = joinTokens(sett.tokens);
		if( sett.tokens.empty() )
			return {};
		if( sett.tokens.size() > 50 )
			return {};
		
		typedef Result<typename DbType::TStore::TDoc> TRes;

		// load
		std::vector<TRes> arr;
		for( size_t i=0; i<dbs.size(); ++i ) {
			auto res = dbs[i]->findMatchAll(sett);
			for( const auto& id : res )
			{
				auto pair = dbs[i]->store().findDoc(id);
				arr.emplace_back(i, id, arr.size(), pair->first, pair->second);
			}
		}

		// limit todo??
		//if( start >= arr.size() )
		//	return {};

		// filter before sort
		if(sett.funcFilter) {
			arr.erase(
				std::remove_if(
					arr.begin(),
					arr.end(),
					[&sett](const TRes& doc){return !sett.funcFilter(doc);}
				),
				arr.end()
			);
			// update index
			size_t i = 0;
			for(auto& x : arr) {
				x.index = i;
				i++;
			}
		}

		// sort
		sortDocs<TRes>(arr, sett, cmps...);
		
		return arr;
	}
}

