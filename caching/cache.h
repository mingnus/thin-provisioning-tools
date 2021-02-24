#ifndef CACHE_DEVICE_H
#define CACHE_DEVICE_H

#include "caching/metadata.h"

namespace caching {
	class cache {
	public:
		typedef std::shared_ptr<cache> ptr;

		cache(block_manager::ptr bm);

		cache(block_manager::ptr,
		      sector_t data_block_size,
		      block_address nr_cache_blocks);

		void commit();

		void set_needs_check();

	private:
		metadata::ptr md_;
	};
}

#endif
