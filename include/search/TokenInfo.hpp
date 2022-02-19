//
//  TokenInfo.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>

namespace Search {

template <typename T> struct TokenInfo {
  T docId;
  bool isWhole;
};

template <class T>
bool operator==(const TokenInfo<T>& a, const TokenInfo<T>& b) {
  return a.docId == b.docId && a.isWhole == b.isWhole;
}

template <class T>
bool operator!=(const TokenInfo<T>& a, const TokenInfo<T>& b) {
  return !(a == b);
}

} // namespace Search