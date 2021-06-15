//
//  KeyValueMemory.hpp
//
//  Created by Ignac Banic on 11/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <vector>
#include <tsl/hopscotch_map.h>
#include <filesystem>
#include <search/KeyValueFile.hpp>

namespace Search {


	class KeyValueMemory {
		tsl::hopscotch_map<std::string, Bytes> map_;

	public:
		void set(const std::string& key, const Bytes& data) {
			map_[key] = data;
		}

		void remove(const std::string& key) {
			auto ptr = map_.find(key);
			if( ptr != map_.end() ) {
				map_.erase(ptr);
			}
		}

		BytesView get(const std::string& key) {
			auto ptr = map_.find(key);
			if( ptr != map_.end() ) {
				return BytesView(ptr->second.data(), ptr->second.size());
			}
			return {};
		}

		void writeToFile(const fs::path& path) {
			KeyValueFile fl(path);
			fl.lockTableForNumItems(map_.size());
			for( const auto& pair : map_ ) {
				fl.set(pair.first, pair.second);
			}
			fl.unlockTable();
		}
	};

}
