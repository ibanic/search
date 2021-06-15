#include "LoadExcerpt.hpp"

namespace Search {
	Range mergeRange(const Range& r1, const Range& r2) {
		auto s1 = r1.start;
		auto e1 = r1.end;
		auto s2 = r2.start;
		auto e2 = r2.end;

		if( s2 < s1 ) {
			auto tmp = s1;
			s1 = s2;
			s2 = tmp;
			tmp = e1;
			e1 = e2;
			e2 = tmp;
		}
		if( s2 <= e1 ) {
			Range r3;
			r3.start = s1;
			r3.end = std::max(e1, e2);
			return r3;
		}
		return Range();
	}

	Range makeRange(uint32_t idx, const std::vector<std::string>& txt)
	{
		uint32_t start, end;
		if( idx >= 3 )
			start = idx-3;
		else
			start = 0;
		if( idx+3 < txt.size() )
			end = idx + 3;
		else
			end = txt.size()-1;
		return Range(start, end);
	}

	std::string textPart(const std::vector<std::string>& txts, const Range& r)
	{
		std::string fin;
		for( uint32_t i=r.start; i<=r.end; ++i ) {
			if( i != r.start )
				fin += " ";
			fin += txts[i];
		}
		return fin;
	}

	std::string excerptToString(const std::vector<ExcerptPart>& parts)
	{
		std::string txt;
		for( const auto& p : parts ) {
			if( p.type == ExcPartType2::Regular ) {
				txt += p.text;
			}
			else if( p.type == ExcPartType2::Highlighted ) {
				txt += p.text;
			}
			else if( p.type == ExcPartType2::Dots ) {
				txt += " ... ";
			}
			else {
				throw std::runtime_error("unknown excerpt part");
			}
		}
		return txt;
	}


	std::vector<ExcerptPart> loadExcerpt(const std::vector<std::string>& docTokens, const std::vector<std::string>& searchTokens)
	{
		std::string allTokens2 = joinTokens(docTokens);
		auto allTokens = splitTokens(allTokens2);
		std::vector<Range> rngs;
		for( const auto& token : searchTokens ) {
			auto ptr = std::find(allTokens.begin(), allTokens.end(), token);
			if( ptr == allTokens.end() )
				continue;
			size_t idx = ptr - allTokens.begin();
			auto r = makeRange(idx, allTokens);
			bool found = false;
			for( size_t j=0; j<rngs.size(); ++j ) {
				auto r2 = mergeRange(rngs[j], r);
				if( r2.end != 0 && r2.start != r2.end ) {
					rngs[j] = r2;
					found = true;
					break;
				}
			}
			if(!found) {
				rngs.push_back(r);
			}
		}
		// add to
		std::vector<ExcerptPart> arr;
		for( const auto& r2 : rngs ) {
			if( !arr.empty() ) {
				ExcerptPart dots;
				dots.type = ExcPartType2::Dots;
				arr.push_back(dots);
			}
			ExcerptPart part;
			part.type = ExcPartType2::Regular;
			part.text = textPart(allTokens, r2);
			arr.push_back(part);
		}
		return arr;
	}
}
