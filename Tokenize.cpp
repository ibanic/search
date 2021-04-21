#include "Tokenize.hpp"
//unistr.h or ustring.h
#include <unicode/unistr.h>
#include <unicode/translit.h>
#include <unicode/utypes.h>
#include <stdexcept>
#include <algorithm>

#include "unicode/utf8.h"
#include "unicode/uchar.h"
#include <mutex>

using namespace icu;

namespace Search {

	uint8_t charLen(char ch)
	{	
		if(ch<=127) return 1;
		else if ((ch & 0xE0) == 0xC0) return 2;
		else if ((ch & 0xF0) == 0xE0) return 3;
		else if ((ch & 0xF8) == 0xF0) return 4;
		//else if ((ch & 0xFC) == 0xF8) return 5; // 111110bb //byte 5, unnecessary in 4 byte UTF-8
		//else if ((ch & 0xFE) == 0xFC) return 6; // 1111110b //byte 6, unnecessary in 4 byte UTF-8
		throw std::runtime_error("charLen() error");
	}

	bool isWhitespace(const char* c) {
		UChar32 c3;
		const uint8_t* c2 = (const uint8_t*)c;
		int32_t ccc = 0;
		U8_NEXT_UNSAFE(c2, ccc, c3);
		if(u_isUWhiteSpace(c3))
			return true;
		return false;
	}

	std::vector<std::string> tokenize(std::string_view txt)
	{
		return tokenize(std::string(txt.data(), txt.size()));
	}

	uint8_t charLenToRemove(const char* ch) {
		char c = *ch;
		if(c=='.' || c==',' || c=='!' || c=='?' || c==':' || c==';' || c=='&' || c=='"' || c=='\'' || c=='(' || c==')') {
			return 1;
		}
		return 0;
	}

	void clearToken(std::string& txt)
	{
		// start
		while(!txt.empty()) {
			auto l = charLenToRemove(&txt[0]);
			if( l == 0 )
				break;
			txt.erase(txt.begin(), txt.begin()+l);
		}
		// end
		size_t lastToErase = std::string::npos;
		for( size_t i=0; i<txt.size(); ) {
			if( charLenToRemove(&txt[i]) == 0 ) {
				lastToErase = std::string::npos;
			}
			else {
				if( lastToErase == std::string::npos ) {
					lastToErase = i;
				}
			}
			i += charLen(txt[i]);
		}
		if( lastToErase != std::string::npos ) {
			txt.erase(txt.begin()+lastToErase, txt.end());
		}
	}

	std::vector<std::string> tokenize(const std::string& txt)
	{
		auto txt2 = UnicodeString::fromUTF8(txt);
		
		{
			// not sure abouth thread safety
			// in docs there is: Transliterator objects are stateless; they retain no information between calls to transliterate()
			// in some docs there is also: However, this does not mean that threads may share transliterators without synchronizing them
			static std::mutex mx;
			const std::lock_guard<std::mutex> lock(mx);

			// Transliterate UTF-16 UnicodeString
			static UErrorCode status = U_ZERO_ERROR;
			//static Transliterator *accentsConverter = Transliterator::createInstance("NFD; [:M:] Remove; NFC", UTRANS_FORWARD, status);
			static std::unique_ptr<Transliterator> accentsConverter(Transliterator::createInstance("NFD; [:M:] Remove; NFC", UTRANS_FORWARD, status));
			accentsConverter->transliterate(txt2);
		}
		txt2.toLower();
		std::string txt3;
		txt2.toUTF8String(txt3);

		std::vector<std::string> res;
		size_t start = std::string::npos;
		for( size_t i=0; i<=txt3.size(); ) {
			if( i == txt3.size() ) {
				if( start != std::string::npos ) {
					auto txt4 = txt3.substr(start, i-start);
					clearToken(txt4);
					if( !txt4.empty() ) {
						res.push_back(txt4);
					}
					start = std::string::npos;
				}
				i += 1;
			}
			else if( isWhitespace(&txt3[i]) ) {
				if( start != std::string::npos ) {
					auto txt4 = txt3.substr(start, i-start);
					clearToken(txt4);
					if( !txt4.empty() ) {
						res.push_back(txt4);
					}
					start = std::string::npos;
				}
				i += charLen(txt3[i]);
			}
			else {
				if( start == std::string::npos ) {
					start = i;
				}
				i += charLen(txt3[i]);
			}
		}
		return res;
	}

	std::string joinTokens(const std::vector<std::string>& tokens)
	{
		std::string txt;
		for( const auto& tk : tokens ) {
			if(!txt.empty() && !tk.empty())
				txt += " ";
			txt += tk;
		}
		return txt;
	}

	std::vector<std::string> splitTokens(std::string_view txt)
	{
		if(txt.empty())
			return {};
		std::vector<std::string> arr;
		size_t start = 0;
		for( size_t i=0; i<=txt.size(); ) {
			if( i == txt.size() || txt[i] == ' ' ) {
				// delimiter
				if(i-start == 0) {
					throw std::runtime_error("splitTokens() zero len token");
				}
				arr.push_back(std::string(txt.data()+start, i-start));
				start = i+1;
				i += 1;
			}
			else {
				i += charLen(txt[i]);
			}
		}
		return arr;
	}

	bool tokensOverlap(std::string_view all, std::string_view search) {
		size_t pos = 0;
		while(pos < all.size()) {
			pos = all.find(search, pos);
			if( pos == std::string::npos ) {
				return false;
			}
			if( pos != 0 ) {
				if( all[pos-1] != ' ' ) {
					pos += search.size();
					continue;
				}
			}
			if( pos+search.size() < all.size() ) {
				if( all[pos+search.size()] != ' ' ) {
					pos += search.size();
					continue;
				}
			}
			return true;
		}
		return false;
	}

size_t numTokensOverlap(const std::string& all, const std::string& search)
{
	auto arr1 = splitTokens(all);
	auto arr2 = splitTokens(search);
	size_t num = 0;
	for( const auto& t : arr1 ) {
		auto ptr = std::find(arr2.begin(), arr2.end(), t);
		if(ptr != arr2.end()) {
			num++;
		}
	}
	return num;
}

}
