//
//  FindMany.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <search/SearchSettings.hpp>
#include <search/Sort.hpp>
#include <search/Tokenize.hpp>

#include <algorithm>

namespace Search {

template <class DbType, class... Ts>
std::vector<Result<typename DbType::TStore::TDoc>>
findMany(const std::vector<const DbType*>& dbs,
         SearchSettings<typename DbType::TStore::TDoc>& sett, Ts&... cmps) {
  // Tokenize: ?
  // Load tokens: 62 %
  // Load docs: 28 %
  // Sort: 10 %

  sett.tokens = tokenize(sett.query);
  sett.tokensJoined = joinTokens(sett.tokens);
  if (sett.tokens.empty()) {
    return {};
  }
  if (sett.tokens.size() > 50) {
    return {};
  }

  typedef Result<typename DbType::TStore::TDoc> TRes;

  // load
  std::vector<TRes> arr;
  for (size_t i = 0; i < dbs.size(); ++i) {
    auto res = dbs[i]->findMatchAll(sett);
    for (const auto& id : res) {
      auto pair = dbs[i]->store().findDoc(id);
      arr.emplace_back(i, id, arr.size(), pair->first, pair->second);
    }
  }

  // filter before sort
  if (sett.funcFilter) {
    arr.erase(std::remove_if(
                  arr.begin(), arr.end(),
                  [&sett](const TRes& doc) { return !sett.funcFilter(doc); }),
              arr.end());
    // update index
    size_t i = 0;
    for (auto& x : arr) {
      x.index = i;
      i++;
    }
  }

  // sort
  sortDocs<TRes>(arr, sett, cmps...);

  return arr;
}
} // namespace Search
