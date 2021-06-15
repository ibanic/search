//
//  DocSimple.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <array>
#include "Types.hpp"
#include <cstring>

namespace Search {


	class DocSimple {
	public:
		typedef uint32_t TId;
		typedef std::array<std::byte, sizeof(TId)> TIdSerialized;
		
	private:
		TId id_;
		std::string text_;
		
	public:
		DocSimple(TId id, const std::string& text)
			: id_(id), text_(text)
		{
		}
		
		DocSimple(TId id, BytesView dt2)
			: id_(id)
		{
			const std::byte* data = dt2.data();
			uint32_t s;
			std::memcpy(static_cast<void*>(&s), &data[0], sizeof(uint32_t));
			text_.resize(s);
			std::memcpy(static_cast<void*>(&text_[0]), &data[sizeof(uint32_t)], s);
		}
		
		Bytes serialize() const {
			Bytes b(text_.size()+sizeof(uint32_t), (std::byte)' ');
			uint32_t s = text_.size();
			std::memcpy(b.data(), static_cast<const void*>(&s), sizeof(uint32_t));
			std::memcpy(b.data()+sizeof(uint32_t), text_.data(), text_.size());
			return b;
		}
		static TIdSerialized serializeId(TId id) {
			TIdSerialized arr;
			std::memcpy(&arr[0], (void*)&id, sizeof(TId));
			return arr;
		}
		static TId deserializeId(const TIdSerialized& arr)
		{
			TId id;
			std::memcpy(&id, &arr[0], sizeof(TId));
			return id;
		}

		TId docId() const { return id_; }
		std::vector<std::string> allTexts() const {
			return {text_};
		}
	};


}
