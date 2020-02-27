#include <algorithm>
#include <stdexcept>
#include <limits>

#include "thin-provisioning/space_map_rebuilder.h"

#include "persistent-data/data-structures/btree_counter.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "persistent-data/space_map.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/metadata_counter.h"

using namespace base;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	using namespace superblock_detail;

	//--------------------------------

	class fatal_damage_visitor {
	public:
		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			throw std::runtime_error("metadata contains errors (run thin_check for details).");
		}
	};

	//--------------------------------

	class data_space_map_creator: public mapping_tree_detail::mapping_visitor {
	public:
		data_space_map_creator(checked_space_map::ptr sm)
			: sm_(sm) {
		}

		void visit(btree_path const &path,
			   mapping_tree_detail::block_time const &m)
		{
			sm_->inc(m.block_);
		}

	private:
		checked_space_map::ptr sm_;
	};

	//--------------------------------

	space_map::ptr
	rebuild_core_map(transaction_manager::ptr tm,
			 superblock_detail::superblock const &sb,
			 nested_output &out)
	{
		out << "scanning existed mapping trees" << end_message();
		out.inc_indent();

		block_address nr_blocks = tm->get_bm()->get_nr_blocks();
		space_map::ptr core(new core_map(nr_blocks));

		block_counter bc(true); // disallow broken nodes
		count_trees(tm, sb, bc);

		for (block_address b = 0; b < nr_blocks; b++)
			core->set_count(b, bc.get_count(b));

		tm->set_sm(core);

		out.dec_indent();
		return core;
	}

	// reserve the blocks used by space maps, by setting
	// their reference counts to 1
	space_map::ptr
	reserve_space_maps(transaction_manager::ptr tm,
			   superblock_detail::superblock const &sb,
			   nested_output &out)
	{
		out << "reserving obsoleted space maps" << end_message();
		out.inc_indent();

		block_address nr_blocks = tm->get_bm()->get_nr_blocks();
		space_map::ptr core = tm->get_sm();
		space_map::ptr space_map_reserved(new core_map(nr_blocks));

		block_counter bc; // allow broken nodes
		count_space_maps(tm, sb, bc);

		// FIXME: use iterator to improve performance
		for (block_address b = 0; b < nr_blocks; b++) {
			if (!core->get_count(b) && bc.get_count(b)) {
				core->set_count(b, 1);
				space_map_reserved->set_count(b, 1);
			}
		}

		out.dec_indent();
		return space_map_reserved;
	}

	void copy_space_maps(space_map::ptr lhs, space_map::ptr rhs) {
		block_address nr_blocks = lhs->get_nr_blocks();

		for (block_address b = 0; b < nr_blocks; b++) {
			uint32_t count = rhs->get_count(b);
			if (count > 0)
				lhs->set_count(b, rhs->get_count(b));
		}
	}

	void subtract_space_maps(space_map::ptr lhs, space_map::ptr rhs) {
		block_address nr_blocks = lhs->get_nr_blocks();

		for (block_address b = 0; b < nr_blocks; b++) {
			if (rhs->get_count(b))
				lhs->set_count(b, 0);
		}
	}

	checked_space_map::ptr
	rebuild_metadata_space_map(transaction_manager::ptr tm,
				   superblock_detail::superblock const &sb,
				   nested_output &out)
	{
		out << "rebuilding metadata space map" << end_message();
		out.inc_indent();

		space_map::ptr core = rebuild_core_map(tm, sb, out);
		space_map::ptr space_map_reserved = reserve_space_maps(tm, sb, out);

		out << "creating the new metadata space map" << end_message();
		checked_space_map::ptr metadata_sm = create_metadata_sm(*tm, core->get_nr_blocks());
		copy_space_maps(metadata_sm, core);
		tm->set_sm(metadata_sm);

		// free the blocks belonging to obsoleted space maps
		subtract_space_maps(metadata_sm, space_map_reserved);

		out.dec_indent();
		return metadata_sm;
	}

	checked_space_map::ptr
	rebuild_data_space_map(transaction_manager::ptr tm,
			       superblock_detail::superblock const &sb,
			       nested_output &out)
	{
		out << "rebuilding data space map" << end_message();
		out.inc_indent();

		out << "creating the new data space map" << end_message();
		sm_disk_detail::sm_root root;
		unpack_sm_root(sb.data_space_map_root_, root);
		checked_space_map::ptr data_sm = create_disk_sm(*tm, root.nr_blocks_);

		out << "filling the new data space map" << end_message();
		data_space_map_creator v(data_sm);
		fatal_damage_visitor dv;
		mapping_tree mtree(*tm, sb.data_mapping_root_,
			mapping_tree_detail::block_time_ref_counter(data_sm));
		btree_visit_values(mtree, v, dv);

		out.dec_indent();
		return data_sm;
	}

	void commit_metadata(transaction_manager::ptr tm,
			     superblock_detail::superblock &sb,
			     checked_space_map::ptr metadata_sm,
			     checked_space_map::ptr data_sm,
			     nested_output &out)
	{
		out << "committing metadata" << end_message();
		out.inc_indent();

		block_manager<>::ptr bm = tm->get_bm();

		data_sm->commit();
		data_sm->copy_root(&sb.data_space_map_root_, sizeof(sb.data_space_map_root_));

		metadata_sm->commit();
		metadata_sm->copy_root(&sb.metadata_space_map_root_, sizeof(sb.metadata_space_map_root_));
		sb.metadata_nr_blocks_ = metadata_sm->get_nr_blocks();

		write_superblock(bm, sb);

		out.dec_indent();
	}
}

namespace thin_provisioning {
	void rebuild_all_space_maps(block_manager<>::ptr bm, nested_output &out) {
		transaction_manager::ptr tm = persistent_data::open_tm(bm, superblock_detail::SUPERBLOCK_LOCATION);
		superblock_detail::superblock sb = read_superblock(bm, superblock_detail::SUPERBLOCK_LOCATION);

		checked_space_map::ptr metadata_sm = rebuild_metadata_space_map(tm, sb, out);
		checked_space_map::ptr data_sm = rebuild_data_space_map(tm, sb, out);

		commit_metadata(tm, sb, metadata_sm, data_sm, out);
	}
};
