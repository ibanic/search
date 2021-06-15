#include <iostream>
#include <iostream>
#include <filesystem>
#include <vector>
#include <future>

#include <search/FileStore.hpp>
#include <search/MemoryStore.hpp>
#include <search/Db.hpp>
#include <search/FindMany.hpp>
#include <search/Comparators.hpp>


namespace fs = std::filesystem;
using namespace Search;
std::string_view separator("+ * + * + * + * + * + * + * + * + * + *\n");


void test_search(const fs::path&);
void test_index(const fs::path&, const fs::path&);
void test_index_single_thread(const fs::path&, const fs::path&);


int main(int argc, char *argv[]) {
	fs::path test11_input(fs::current_path() / "wiki-cleaned.txt");
	fs::path test11_output(fs::current_path() / "wiki-out");
	
	auto pth2 = test11_output;
	pth2 += ".tokens";
	
	if(!fs::is_regular_file(test11_input)) {
		std::cout << "Missing file " << test11_input << "\n";
		std::cout << "download https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2 and extract\n";
		std::cout << "run ./1-convert.py\n";
		return 1;
	}
	
	std::cout << "Enter option:\n1 - make index\n2 - search\n";
	std::string opt;
	std::cin >> opt;
	
	if(opt == "1") {
		test_index(test11_input, test11_output);
		return 0;
	}
	
	if(opt == "2") {
		test_search(test11_output);
		return 0;
	}
	
	std::cout << "Unknown option\n";
	return 1;
}





template <typename T>
T readNum(BytesView& buff) {
	T n;
	std::memcpy((void*)&n, buff.data(), sizeof(T));
	buff.remove_prefix(sizeof(T));
	return n;
}

std::string readString(BytesView& buff) {
	auto l = readNum<uint32_t>(buff);
	std::string txt(l, 'x');
	std::memcpy(txt.data(), buff.data(), l);
	buff.remove_prefix(l);
	return txt;
}

template <typename T>
void serializeNumber(Bytes& buff, T num) {
	auto s = buff.size();
	buff.resize(s+sizeof(T));
	std::memcpy(&buff[s], (void*)&num, sizeof(T));
}

void serializeString(Bytes& buff, const std::string& txt) {
	serializeNumber<uint32_t>(buff, txt.size());
	auto s = buff.size();
	if( txt.size() != 0 ) {
		buff.resize(s+txt.size());
		std::memcpy(&buff[s], (void*)txt.data(), txt.size());
	}
}


const char* testFindStart(const char* dt, const char* end) {
	std::string_view dt2(dt, end-dt);
	auto res = dt2.find(separator);
	if( res == std::string_view::npos ) {
		return nullptr;
	}
	return dt + res;
}




class Doc {
public:
	typedef uint32_t TId;
	typedef std::array<std::byte, sizeof(TId)> TIdSerialized;
	
	TId id_;
	std::string title_;
	std::string text_;
	
	Doc() : id_(0) { }
	Doc(uint32_t id, std::string_view title, std::string_view text) : id_(id), title_(title), text_(text) {}

	Doc(TId id2, BytesView dt2) : id_(id2) {
		title_ = readString(dt2);
		text_ = readString(dt2);
		assert(dt2.empty());
	}
	Bytes serialize() const {
		Bytes data;
		serializeString(data, title_);
		serializeString(data, text_);
		return data;
	}
	static TIdSerialized serializeId(TId id) {
		TIdSerialized arr;
		std::memcpy(&arr[0], &id, sizeof(TId));
		return arr;
	}
	static TId deserializeId(const TIdSerialized& id2) {
		TId id;
		std::memcpy(&id, &id2[0], sizeof(TId));
		return id;
	}
	
	TId docId() const { return id_; }
	
	std::vector<std::string> allTexts() const {
		std::vector<std::string> arr;
		arr.push_back(title_);
		arr.push_back(text_);
		return arr;
	}

	std::string_view title() const {
		return title_;
	}

	std::string_view text() const {
		return text_;
	}
};



std::string_view getLine3(const char* start, const char* end)
{
	std::string_view text(start, end-start);
	auto res = text.find('\n');
	if(res == std::string_view::npos) {
		return text;
	}
	return {start, res+1};
}

void testReadFile(Db<FileStore<Doc>>::BulkWriter& writer, const char* start, const char* end)
{
	Doc doc;
	size_t i = 0;
	while(start < end)
	{
		auto line = getLine3(start, end);
		start += line.size();
		if(line == separator) {
			writer.add(doc);
			doc = Doc();
			i = 0;
			continue;
		}

		if(i == 0) {
			doc.title_ = line;
			// remove new line
			doc.title_.resize(doc.title_.size()-1);
		}
		else if(i == 1) {
			std::string line2(line.data(), line.size()-1);
			doc.id_ = std::stoull(line2);
		}
		else {
			doc.text_.append(line);
		}
		i++;
	}
}





void test_index(const fs::path& input, const fs::path& output) {
	FileStore<Doc>::removeFiles(output);

	typedef Db<FileStore<Doc>> TSearchDb;
	FileStore<Doc> store(output);
	TSearchDb db(store);

	if(!fs::is_regular_file(input)) {
		throw std::runtime_error("Missing source file");
	}
	boost::iostreams::mapped_file_source file;
	file.open(input);
	if( !file.is_open() || file.data() == nullptr )
		throw std::runtime_error("Cant open file");

	auto numThreads = std::thread::hardware_concurrency();
	if(numThreads==0) {numThreads = 1;}
	const char* end = file.data()+file.size();
	std::vector<const char*> starts(numThreads+1);
	size_t perThread = file.size() / numThreads;
	for( size_t i=0; i<numThreads; ++i ) {
		auto start2 = file.data() + i * perThread;
		if( i == 0 ) {
			starts[i] = start2;
		}
		else {
			starts[i] = testFindStart(start2, end);
			assert(starts[i] != nullptr);
		}
	}
	starts[numThreads] = end;

	std::cout << "Reading ...\n";
	auto t1 = std::chrono::high_resolution_clock::now();
	auto writers = db.bulkWriters(numThreads);
	std::vector<std::future<void>> arr(numThreads);
	for( size_t i=0; i<numThreads; ++i ) {
		arr[i] = std::async(std::launch::async, testReadFile, std::ref(writers[i]), starts[i], starts[i+1]);
	}
	for(auto& ft : arr) {
		ft.wait();
	}
	auto t2 = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> d2 = t2 - t1;
	std::cout << "Read in " << d2.count() << " sec\n";

	db.bulkAdd(writers);
	
	// - . - . - . - . - . - . -
	
	typedef Result<Doc> TRes;
	CompIsWhole<TRes> cmp1;
	CompWordsTogether<TRes> cmp2;
	SearchSettings<Doc> sett;
	sett.query = "slov";
	auto results1 = findMany<TSearchDb> ({&db}, sett, cmp1, cmp2);
	assert(!results1.empty());
}



void test_index_single_thread(const fs::path& input, const fs::path& output)
{
	FileStore<Doc>::removeFiles(output);

	typedef Db<FileStore<Doc>> TSearchDb;
	FileStore<Doc> store(output);
	TSearchDb db(store);

	if(!fs::is_regular_file(input)) {
		throw std::runtime_error("Missing source file");
	}
	boost::iostreams::mapped_file_source file;
	file.open(input);
	if( !file.is_open() || file.data() == nullptr )
		throw std::runtime_error("Cant open file");
	
	Doc doc;
	size_t i = 0;
	
	uint prc = 0;
	
	auto start = file.data();
	auto end = start + file.size();
	while(start < end)
	{
		auto line = getLine3(start, end);
		start += line.size();
		if(line == separator) {
			db.add(doc);
			doc = Doc();
			i = 0;
			continue;
		}

		if(i == 0) {
			doc.title_ = line;
			// remove new line
			doc.title_.resize(doc.title_.size()-1);
		}
		else if(i == 1) {
			std::string line2(line.data(), line.size()-1);
			doc.id_ = std::stoull(line2);
		}
		else {
			doc.text_.append(line);
		}
		i++;
		
		auto prc2 = (int)std::round((double)(start-file.data())/(end-file.data()) * 100);
		if(prc2 != prc) {
			prc = prc2;
			std::cout << prc << std::endl;
		}
	}
}










void test_search(const fs::path& output) {
	typedef Db<FileStore<Doc>> TSearchDb;
	FileStore<Doc> store(output);
	TSearchDb db(store);

	typedef Result<Doc> TRes;
	CompIsWhole<TRes> cmp1;
	CompWordsTogether<TRes> cmp2;

	while(true) {
		std::cout << "Enter search query: ";
		std::string q;
		std::cin >> q;
	
		auto start = std::chrono::high_resolution_clock::now();
		SearchSettings<Doc> sett;
		sett.query = q;
		auto results3 = findMany<TSearchDb> ({&db}, sett, cmp1, cmp2);
		size_t i = 0;
		for( const auto& res : results3 ) {
			std::cout << res.doc.docId() << "\t" << res.doc.title() << "\n";
			i++;
			if( i == 20 ) { break; }
		}

		auto finish = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> elapsed = finish - start;
		std::cout << "in " << (elapsed.count()*1000) << " ms  - - - - - - -\n";
	}
}




