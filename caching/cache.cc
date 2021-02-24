#include "caching/cache.h"

using namespace caching;

namespace {
	void write_hints(metadata::ptr md) {
		size_t hint_width = 4; // TODO: cache policy 

		md->setup_hint_array(hint_width);
		vector<unsigned char> hint_value(hint_width, '\0');
		md->hints_->grow(md->mappings_->get_nr_entries(), hint_value);
	}
}

cache::cache(block_manager::ptr bm)
{
	md_ = metadata::ptr(new metadata(bm));
}

cache::cache(block_manager::ptr bm,
	     sector_t data_block_size,
	     block_address nr_cache_blocks)
{
	md_ = metadata::ptr(new metadata(bm, metadata::CREATE, 2, data_block_size));
	md_->resize(nr_cache_blocks);
	write_hints(md_); // FIXME: stub
	md_->commit();
}

void
cache::commit()
{
	md_->commit();
}

void
cache::set_needs_check()
{
	md_->sb_.flags.set_flag(superblock_flags::NEEDS_CHECK);
}
