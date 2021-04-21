//
//  LoadExcerpt.hpp
//
//  Created by Ignac Banic on 2/01/20.
//  Copyright © 2020 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <map>
#include <vector>
#include <algorithm>
#include "Tokenize.hpp"

namespace Search {

	
	enum class ExcPartType2 { Regular = 0, Highlighted = 1, Dots = 2 };
	struct ExcerptPart {
		ExcPartType2 type;
		std::string text;
	};

	std::string excerptToString(const std::vector<ExcerptPart>& parts);



	struct Range {
		uint32_t start = 0;
		uint32_t end = 0;
		Range() {}
		Range(uint32_t s, uint32_t e) : start(s), end(e) { }
	};

	Range mergeRange(const Range& r1, const Range& r2);
	Range makeRange(uint32_t idx, const std::vector<std::string>& txt);
	std::string textPart(const std::vector<std::string>& txts, const Range& r);


	
	std::vector<ExcerptPart> loadExcerpt(const std::vector<std::string>& docTokens, const std::vector<std::string>& searchTokens);




}
