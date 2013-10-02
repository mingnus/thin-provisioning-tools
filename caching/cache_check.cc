#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/error_state.h"
#include "base/nested_output.h"
#include "caching/metadata.h"
#include "persistent-data/block.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"

using namespace boost;
using namespace caching;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {

	class reporter_base {
	public:
		reporter_base(nested_output &o)
		: out_(o),
		  err_(NO_ERROR) {
		}

		virtual ~reporter_base() {}

		nested_output &out() {
			return out_;
		}

		nested_output::nest push() {
			return out_.push();
		}

		base::error_state get_error() const {
			return err_;
		}

		void mplus_error(error_state err) {
			err_ = combine_errors(err_, err);
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	class superblock_reporter : public superblock_detail::damage_visitor, reporter_base {
	public:
		superblock_reporter(nested_output &o)
		: reporter_base(o) {
		}

		virtual void visit(superblock_detail::superblock_corruption const &d) {
			out() << "superblock is corrupt" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.desc_ << end_message();
			}

			mplus_error(FATAL);
		}

		using reporter_base::get_error;
	};

	class mapping_reporter : public reporter_base {
	public:
		mapping_reporter(nested_output &o)
		: reporter_base(o) {
		}
	};

	class hint_reporter : public reporter_base {
	public:
		hint_reporter(nested_output &o)
		: reporter_base(o) {
		}
	};

	//--------------------------------

	transaction_manager::ptr open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(superblock_detail::SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	//--------------------------------

	struct flags {
		flags()
			: check_mappings_(false),
			  check_hints_(false),
			  ignore_non_fatal_errors_(false),
			  quiet_(false) {
		}

		bool check_mappings_;
		bool check_hints_;
		bool ignore_non_fatal_errors_;
		bool quiet_;
	};

	struct stat guarded_stat(string const &path) {
		struct stat info;

		int r = ::stat(path.c_str(), &info);
		if (r) {
			ostringstream msg;
			char buffer[128], *ptr;

			ptr = ::strerror_r(errno, buffer, sizeof(buffer));
			msg << path << ": " << ptr;
			throw runtime_error(msg.str());
		}

		return info;
	}

	error_state metadata_check(block_manager<>::ptr bm, flags const &fs) {
		error_state err = NO_ERROR;

		nested_output out(cerr, 2);
		if (fs.quiet_)
			out.disable();

		superblock_reporter sb_rep(out);
		mapping_reporter mapping_rep(out);
		hint_reporter hint_rep(out);

		out << "examining superblock" << end_message();
		{
			nested_output::nest _ = out.push();
			check_superblock(bm, sb_rep);
		}

		if (sb_rep.get_error() == FATAL)
			return FATAL;

		superblock_detail::superblock sb = read_superblock(bm);
		transaction_manager::ptr tm = open_tm(bm);

		if (fs.check_mappings_) {
			out << "examining mapping array" << end_message();
			{
				nested_output::nest _ = out.push();
				mapping_array ma(tm, mapping_array::ref_counter(), sb.mapping_root, sb.cache_blocks);
				// check_mapping_array(ma, mapping_rep);
			}
		}

		if (fs.check_hints_) {
			out << "examining hint array" << end_message();
			{
				nested_output::nest _ = out.push();
			}
		}

		return err;
	}

	int check(string const &path, flags const &fs) {
		struct stat info = guarded_stat(path);

		if (!S_ISREG(info.st_mode) && !S_ISBLK(info.st_mode)) {
			ostringstream msg;
			msg << path << ": " << "Not a block device or regular file";
			throw runtime_error(msg.str());
		}

		try {
			block_manager<>::ptr bm = open_bm(path, block_io<>::READ_ONLY);
			//metadata::ptr md(new metadata(bm, metadata::OPEN));

			error_state err = metadata_check(bm, fs);
#if 0
			if (maybe_errors) {
				if (!fs.quiet_)
					cerr << error_selector(*maybe_errors, 3);
				return 1;
			}
#endif

		} catch (std::exception &e) {
			if (!fs.quiet_)
				cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-q|--quiet}" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl;
	}

	char const *TOOLS_VERSION = "0.1.6";
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	flags fs;
	const char shortopts[] = "qhV";
	const struct option longopts[] = {
		{ "quiet", no_argument, NULL, 'q'},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'q':
			fs.quiet_ = true;
			break;

		case 'V':
			cout << TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	try {
		check(argv[optind], fs);

	} catch (std::exception const &e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}

//----------------------------------------------------------------