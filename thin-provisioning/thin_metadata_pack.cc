// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/optional.hpp>
#include <fcntl.h>
#include <fstream>
#include <getopt.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/checksum.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/superblock.h"
#include "version.h"

using namespace thin_provisioning;
using namespace persistent_data;

using boost::optional;

//---------------------------------------------------------------------------

namespace {
	using namespace std;
	constexpr uint32_t HEADER_SIZE = 4096;
	constexpr uint64_t MAGIC = 0xa537a0aa6309ef77;

	uint32_t const SUPERBLOCK_CSUM_SEED = 160774;
	uint32_t const BITMAP_CSUM_XOR = 240779;
	uint32_t const INDEX_CSUM_XOR = 160478;
	uint32_t const BTREE_CSUM_XOR = 121107;

	// Pack file format
	// ----------------
	// 
	// file := <file-header> <entry>*
	// file-header := MAGIC BLOCK_SIZE NR_BLOCKS NR_ENTRIES
	// entry := BLOCK_NR BYTES

	struct file_header {
		uint64_t magic_;
		uint64_t block_size_;
		uint64_t nr_blocks_;
		uint64_t nr_entries_;
	};

	struct file_header_disk {
		base::le64 magic_;
		base::le64 block_size_;
		base::le64 nr_blocks_;
		base::le64 nr_entries_;
	} __attribute__ ((packed));

	struct file_header_traits {
		static void pack(file_header const &core, file_header_disk &disk);
		static void unpack(file_header_disk const &disk, file_header &core);
	};

	void file_header_traits::pack(file_header const &core, file_header_disk &disk) {
		disk.magic_ = base::to_disk<base::le64>(core.magic_);
		disk.block_size_ = base::to_disk<base::le64>(core.block_size_);
		disk.nr_blocks_ = base::to_disk<base::le64>(core.nr_blocks_);
		disk.nr_entries_ = base::to_disk<base::le64>(core.nr_entries_);
	}

	void file_header_traits::unpack(file_header_disk const &disk, file_header &core) {
		core.magic_ = base::to_cpu<uint64_t>(disk.magic_);
		core.block_size_ = base::to_cpu<uint64_t>(disk.block_size_);
		core.nr_blocks_ = base::to_cpu<uint64_t>(disk.nr_blocks_);
		core.nr_entries_ = base::to_cpu<uint64_t>(disk.nr_entries_);
	}

	struct flags {
		optional<string> input_file_;
		optional<string> output_file_;
	};

	class is_metadata_functor {
	public:
		is_metadata_functor() {
		}

		bool operator() (void const *raw) const {
			uint32_t const *cksum = reinterpret_cast<uint32_t const*>(raw);
			base::crc32c sum(*cksum);
			sum.append(cksum + 1, MD_BLOCK_SIZE - sizeof(uint32_t));

			switch (sum.get_sum()) {
			case SUPERBLOCK_CSUM_SEED:
			case INDEX_CSUM_XOR:
			case BITMAP_CSUM_XOR:
			case BTREE_CSUM_XOR:
				return true;
			default:
				return false;
			}
		}
	};

	void prealloc_file(string const &file, off_t len) {
		int fd = ::open(file.c_str(), O_TRUNC | O_CREAT | O_RDWR, 0666);
		if (fd < 0)
			throw runtime_error("couldn't open output file");

		if (::fallocate(fd, 0, 0, len))
			throw runtime_error("couldn't fallocate");
		::close(fd);
	}

	uint64_t read_u64(istream &in) {
		base::le64 n;
		in.read(reinterpret_cast<char *>(&n), sizeof(n));

		if (!in)
			throw runtime_error("couldn't read u64");

		return base::to_cpu<uint64_t>(n);
	}

	void write_u64(ostream &out, uint64_t n) {
		base::le64 n_le = base::to_disk<base::le64>(n);
		out.write(reinterpret_cast<char *>(&n_le), sizeof(n_le));

		if (!out)
			throw runtime_error("couldn't write u64");
	}

	file_header read_header(istream &in) {
		vector<char> superblock(HEADER_SIZE, 0);
		in.read(superblock.data(), HEADER_SIZE);

		if (!in)
			throw runtime_error("couldn't read header");

		file_header_disk *disk = reinterpret_cast<file_header_disk*>(superblock.data());
		file_header header;
		file_header_traits::unpack(*disk, header);

		if (header.magic_ != MAGIC)
			throw runtime_error("not a pack file");

		return header;
	}

	void write_header(ostream &out, uint64_t block_size, uint64_t nr_blocks) {
		vector<char> superblock(HEADER_SIZE, 0);

		file_header header;
		header.magic_ = MAGIC;
		header.block_size_ = block_size;
		header.nr_blocks_ = nr_blocks;
		header.nr_entries_ = 0;

		file_header_disk *disk = reinterpret_cast<file_header_disk*>(superblock.data());
		file_header_traits::pack(header, *disk);
		out.write(superblock.data(), HEADER_SIZE);

		if (!out)
			throw runtime_error("couldn't write header");
	}

	int pack(flags const &f) {
		using namespace boost::iostreams;

		std::ofstream out_file(*f.output_file_, ios_base::binary);
		boost::iostreams::filtering_ostreambuf out_buf;
		out_buf.push(zlib_compressor());
		out_buf.push(out_file);
		std::ostream out(&out_buf);
		
		block_manager::ptr bm = open_bm(*f.input_file_, block_manager::READ_ONLY, true);

		uint64_t block_size = 4096;
		auto nr_blocks = bm->get_nr_blocks();

		cerr << "nr_blocks = " << nr_blocks << "\n";

		write_header(out_file, block_size, nr_blocks);

		is_metadata_functor is_metadata;
		for (block_address b = 0; b < nr_blocks; b++) {
			auto rr = bm->read_lock(b);

			if (is_metadata(rr.data())) {
				write_u64(out, b);
				out.write(reinterpret_cast<const char *>(rr.data()), block_size);
			}
		} 

		return 0;
	}

	int unpack(flags const &f)
	{
		using namespace boost::iostreams;
		
		ifstream in_file(*f.input_file_, ios_base::binary);
		if (!in_file)
			throw runtime_error("couldn't open pack file");

		filtering_istreambuf in_buf;
		in_buf.push(zlib_decompressor());
		in_buf.push(in_file);
		std::istream in(&in_buf);

		file_header header = read_header(in_file);
		auto block_size = header.block_size_;
		auto nr_blocks = header.nr_blocks_;

		prealloc_file(*f.output_file_, nr_blocks * block_size);
		block_manager bm(*f.output_file_, nr_blocks, 6, block_manager::READ_WRITE, true);
		uint8_t bytes[block_size];
		while (true) {
			uint64_t block_nr;
			try {
				block_nr = read_u64(in);
			} catch (...) {
				break;
			}

			if (block_nr >= nr_blocks)
				throw runtime_error("block nr out of bounds");

			in.read(reinterpret_cast<char *>(bytes), block_size);
			if (!in)
				throw runtime_error("couldn't read data");

			auto wr = bm.write_lock(block_nr);
			memcpy(wr.data(), bytes, block_size);
		}

		return 0;
	}
}

//---------------------------------------------------------------------------

thin_metadata_pack_cmd::thin_metadata_pack_cmd()
	: command("thin_metadata_pack")
{
}

void
thin_metadata_pack_cmd::usage(ostream &out) const {
	out << "Usage: " << get_name() << " [options]\n"
	    << "Options:\n"
 	    << "  {-i|--input} <input metadata (binary format)>\n"
	    << "  {-o|--output} <output packed metadata>\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_metadata_pack_cmd::run(int argc, char **argv)
{
	const char shortopts[] = "hi:o:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	flags f;

	int c;
	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'i':
			f.input_file_ = optarg;
			break;

		case 'o':
			f.output_file_ = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (!f.input_file_) {
		cerr << "No input file provided." << endl;
		usage(cerr);
		return 1;
	}

	if (!f.output_file_) {
		cerr << "No output file providied." << endl;
		usage(cerr);
		return 1;
	}

	return pack(f);
}

//---------------------------------------------------------------------------

thin_metadata_unpack_cmd::thin_metadata_unpack_cmd()
	: command("thin_metadata_unpack")
{
}

void
thin_metadata_unpack_cmd::usage(ostream &out) const {
	out << "Usage: " << get_name() << " [options]\n"
	    << "Options:\n"
 	    << "  {-i|--input} <input packed metadata>\n"
	    << "  {-o|--output} <output metadata (binary format)>\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_metadata_unpack_cmd::run(int argc, char **argv)
{
	const char shortopts[] = "hi:o:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	flags f;

	int c;
	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'i':
			f.input_file_ = optarg;
			break;

		case 'o':
			f.output_file_ = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (!f.input_file_) {
		cerr << "No input file provided." << endl;
		usage(cerr);
		return 1;
	}

	if (!f.output_file_) {
		cerr << "No output file providied." << endl;
		usage(cerr);
		return 1;
	}

	return unpack(f);
}

//---------------------------------------------------------------------------
