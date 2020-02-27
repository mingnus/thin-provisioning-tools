#include "base/nested_output.h"
#include "persistent-data/block.h"

namespace thin_provisioning {
	void rebuild_all_space_maps(persistent_data::block_manager<>::ptr bm,
				    base::nested_output &out);
};
