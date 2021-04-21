//
//  Tokenize.hpp
//
//  Created by Ignac Banic on 31/12/19.
//  Copyright © 2019 Ignac Banic. All rights reserved.
//

#pragma once

#include <string>
#include <vector>

namespace Search {

	uint8_t charLen(char ch);
	std::vector<std::string> tokenize(std::string_view txt);
	std::vector<std::string> tokenize(const std::string& txt);
	std::string joinTokens(const std::vector<std::string>& tokens);
	std::vector<std::string> splitTokens(std::string_view txt);
	bool tokensOverlap(std::string_view all, std::string_view search);
	size_t numTokensOverlap(const std::string& all, const std::string& search);
	
}