//
//  Sort.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <search/Comparators.hpp>
#include <search/SearchSettings.hpp>

#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace Search {

template <class TRes, typename... Ts> struct SortCmp2 {
  std::tuple<Ts...> tup;
  SearchSettings<typename TRes::TDoc>* sett;
  SortCmp2(Ts&... args) : tup(args...) {}

  template <size_t N> bool cmp(const TRes& d1, const TRes& d2) const {
    if (sett && sett->manager && !sett->manager->shouldContinue()) {
      throw SearchCanceledException();
    }
    auto res = std::get<N>(tup).compare(d1, d2);
    if (res != 0) {
      return res < 0;
    }
    if (N + 1 < sizeof...(Ts)) {
      return cmp < N + 1 < sizeof...(Ts) ? N + 1 : N > (d1, d2);
    } else {
      return false;
    }
  }

  inline bool operator()(const TRes& d1, const TRes& d2) {
    return cmp<0>(d1, d2);
  }
};

template <class TRes, class... Ts>
void sortDocs(std::vector<TRes>& arr, SearchSettings<typename TRes::TDoc>& sett,
              Ts&... cmps) {
  if (arr.size() <= 1) {
    return;
  }
  (cmps.init(arr, sett), ...);

  SortCmp2<TRes, Ts...> s1(cmps...);
  s1.sett = &sett;
  std::sort(arr.begin(), arr.end(), s1);

  (cmps.clean(), ...);
}

template <class TRes>
void sortDocs(std::vector<TRes>& arr,
              SearchSettings<typename TRes::TDoc>& sett) {}

} // namespace Search
