//
//  MemoryStore.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <search/TokenInfo.hpp>

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace Search {

template <class TDoc2> class MemoryStore {
public:
  typedef TokenInfo<typename TDoc2::TId> TTokenInfo;
  typedef TDoc2 TDoc;

private:
  std::map<typename TDoc2::TId, TDoc> docs_;
  std::map<typename TDoc2::TId, std::vector<std::string>> docTokens_;
  std::map<std::string, std::vector<TTokenInfo>> index_;

public:
  void addDoc(const typename TDoc2::TId& id, const TDoc& doc,
              const std::vector<std::string>& tokens) {
    docs_.insert({id, doc});
    docTokens_.insert({id, tokens});
  }

  void removeDoc(const typename TDoc2::TId& id) {
    auto ptr = docs_.find(id);
    if (ptr != docs_.end()) {
      docs_.erase(ptr);
    }

    auto ptr2 = docTokens_.find(id);
    if (ptr2 != docTokens_.end()) {
      docTokens_.erase(ptr2);
    }
  }

  std::vector<TDoc> allDocuments() const {
    std::vector<TDoc> arr;
    arr.reserve(docs_.size());
    for (const auto& pair : docs_) {
      arr.push_back(pair.second);
    }
    return arr;
  }

  std::optional<std::pair<TDoc, std::vector<std::string>>>
  findDoc(const typename TDoc2::TId& id) const {
    auto ptr = docs_.find(id);
    if (ptr == docs_.end()) {
      return std::nullopt;
    }
    auto ptr2 = docTokens_.find(id);
    return std::make_pair(ptr->second, ptr2->second);
  }

  void addToken(std::string_view token, const TTokenInfo& info) {
    auto res = index_.insert({std::string(token), {info}});
    if (!res.second) {
      res.first->second.push_back(info);
    }
  }

  void removeToken(std::string_view token, const TTokenInfo& info) {
    auto ptr = index_.find(std::string(token));
    if (ptr == index_.end()) {
      return;
    }
    std::vector<TTokenInfo>& vec = ptr->second;
    auto ptr2 = std::find(vec.begin(), vec.end(), info);
    if (ptr2 != vec.end()) {
      vec.erase(ptr2);
      if (vec.empty()) {
        index_.erase(ptr);
      }
    }
  }

  std::vector<TTokenInfo> findToken(const std::string& token) {
    auto ptr = index_.find(token);
    if (ptr == index_.end()) {
      return {};
    }
    return ptr->second;
  }

  void clear() {
    docs_.clear();
    docTokens_.clear();
    index_.clear();
  }

  size_t sizeDocuments() { return docs_.size(); }

  size_t sizeTokens() { return index_.size(); }
};

} // namespace Search
