#include "base/output_file_requirements.h"
#include "persistent-data/file_utils.h"
#include "caching/cache.h"
#include "caching/commands.h"
#include "version.h"

#include <boost/optional.hpp>
#include <getopt.h>
#include <unistd.h>

using namespace boost;
using namespace caching;

//----------------------------------------------------------------

namespace {
	using base::sector_t;

	struct flags {
		enum metadata_operations {
			METADATA_OP_NONE,
			METADATA_OP_FORMAT,
			METADATA_OP_OPEN,
			METADATA_OP_SET_NEEDS_CHECK,
			METADATA_OP_LAST
		};

		flags()
			: op(METADATA_OP_NONE),
			  cache_block_size(128),
			  nr_cache_blocks(10240)
		{
		}

		bool check_conformance();

		metadata_operations op;
		sector_t cache_block_size;
		block_address nr_cache_blocks;
		optional<uint64_t> trans_id;
		optional<string> output;
	};

	// FIXME: modulize the conditions
	bool flags::check_conformance() {
		if (op == METADATA_OP_NONE || op >= METADATA_OP_LAST) {
			cerr << "Invalid operation." << endl;
			return false;
		}

		if (!output) {
			cerr << "No output file provided." << endl;
			return false;
		} else
			check_output_file_requirements(*output);

		return true;
	}

	//--------------------------------

	cache::ptr open_or_create_cache(flags const &fs) {
		block_manager::ptr bm = open_bm(*fs.output, block_manager::READ_WRITE);

		if (fs.op == flags::METADATA_OP_FORMAT)
			return cache::ptr(new cache(bm, fs.cache_block_size, fs.nr_cache_blocks));
		else
			return cache::ptr(new cache(bm));
	}

	int generate_metadata(flags const &fs) {
		cache::ptr cache = open_or_create_cache(fs);

		switch (fs.op) {
		case flags::METADATA_OP_SET_NEEDS_CHECK:
			cache->set_needs_check();
			break;
		default:
			break;
		}

		cache->commit();

		return 0;
	}
}

//----------------------------------------------------------------

cache_generate_metadata_cmd::cache_generate_metadata_cmd()
	: command("cache_generate_metadata")
{
}

void
cache_generate_metadata_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {--format}\n"
	    << "  {--set-needs-check}\n"
	    << "  {--cache-block-size} <block size>\n"
	    << "  {--nr-cache-blocks} <nr>\n"
	    << "  {-o|--output} <output device or file>\n"
	    << "  {-V|--version}" << endl;
}

int
cache_generate_metadata_cmd::run(int argc, char **argv)
{
	int c;
	struct flags fs;
	const char *shortopts = "ho:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "format", no_argument, NULL, 1 },
		{ "open", no_argument, NULL, 2 },
		{ "set-needs-check", no_argument, NULL, 9 },
		{ "cache-block-size", required_argument, NULL, 1001 },
		{ "nr-cache-blocks", required_argument, NULL, 1002 },
		{ "version", no_argument, NULL, 'V' },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'o':
			fs.output = optarg;
			break;

		case 1:
			fs.op = flags::METADATA_OP_FORMAT;
			break;

		case 2:
			fs.op = flags::METADATA_OP_OPEN;
			break;

		case 9:
			fs.op = flags::METADATA_OP_SET_NEEDS_CHECK;
			break;

		case 1001:
			fs.cache_block_size = parse_uint64(optarg, "cache block size");
			break;

		case 1002:
			fs.nr_cache_blocks = parse_uint64(optarg, "nr cache blocks");
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (!fs.check_conformance()) {
		usage(cerr);
		return 1;
	}

	return generate_metadata(fs);
}

//----------------------------------------------------------------
