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

#include <getopt.h>
#include <string>

#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/space_map_rebuilder.h"
#include "thin-provisioning/superblock.h"
#include "version.h"

using namespace std;
using namespace base;
using namespace persistent_data;
using namespace thin_provisioning;

namespace {
	struct flags {
		flags()
			: quiet(false) {
		}

		bool quiet;
	};


	int rebuild(string const &dev, flags const &fs) {
		nested_output out(cerr, 2);
		if (fs.quiet)
			out.disable();

		try {
			block_manager<>::ptr bm = open_bm(dev, block_manager<>::READ_WRITE);
			rebuild_all_space_maps(bm, out);
		} catch (std::exception &e) {
			out << e.what() << end_message();
			return 1;
		}
		return 0;
	}
}

//----------------------------------------------------------------

thin_rebuild_cmd::thin_rebuild_cmd()
	: command("thin_rebuild")
{
}

void
thin_rebuild_cmd::usage(std::ostream &out) const {
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-q|--quiet}" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_rebuild_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;

	const char shortopts[] = "qhV";
	const struct option longopts[] = {
		{ "quiet", no_argument, NULL, 'q'},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V' },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			usage(cout);
			break;

		case 'q':
			fs.quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr);
		return 1;
	}

	return rebuild(argv[optind], fs);
}

//----------------------------------------------------------------
