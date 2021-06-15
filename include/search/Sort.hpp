//
//  Sort.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <map>
#include <vector>
#include <iostream>
#include <search/SearchSettings.hpp>

#include <search/Comparators.hpp>


namespace Search {


template<class TRes, typename... Ts>
struct SortCmp2 {
	std::tuple<Ts...> tup;
	SearchSettings<typename TRes::TDoc>* sett;
	SortCmp2(Ts&...args) : tup(args...)
	{ }
	
	template <size_t N>
	bool cmp(const TRes& d1, const TRes& d2) const {
		if( sett && sett->manager && !sett->manager->shouldContinue() ) {
			throw SearchCanceledException();
		}
		auto res = std::get<N>(tup).compare(d1, d2);
		if( res != 0 ) {
			return res < 0;
		}
		if( N+1 < sizeof...(Ts) ) {
			return cmp<N+1 < sizeof...(Ts) ? N+1 : N>(d1, d2);
		}
		else {
			return false;
		}
	}
	
	inline bool operator() (const TRes& d1, const TRes& d2) {
		return cmp<0>(d1, d2);
	}

	// not allowed in GCC
	/*template <size_t N>
	bool cmp(const TRes& d1, const TRes& d2) const {
		auto res = std::get<N>(tup).compare(d1, d2);
		if( res != 0 ) {
			return res < 0;
		}
		return cmp<N+1>(d1, d2);
	}
	template<>
	bool cmp<sizeof...(Ts)>(const TRes& d1, const TRes& d2) const {
		return false;
	}*/
};



	template<class TRes, class... Ts>
	void sortDocs(std::vector<TRes>& arr, SearchSettings<typename TRes::TDoc>& sett, Ts& ... cmps)
	{
		if(arr.size() <= 1) {
			return;
		}
		(cmps.init(arr, sett),...);
		
		SortCmp2<TRes, Ts...> s1(cmps...);
		s1.sett = &sett;
		std::sort(arr.begin(), arr.end(), s1);
		
		(cmps.clean(),...);
	}

	template<class TRes>
	void sortDocs(std::vector<TRes>& arr, SearchSettings<typename TRes::TDoc>& sett)
	{
	}


}
