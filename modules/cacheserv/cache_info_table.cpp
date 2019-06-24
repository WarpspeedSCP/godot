#include "cache_info_table.h"

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

DescriptorInfo::DescriptorInfo(FileAccess *fa, part_id new_range) : guid_prefix(new_range), offset(0) {
	ERR_FAIL_COND(!fa);
	internal_data_source = fa;
	total_size = internal_data_source->get_len();

}

Variant DescriptorInfo::to_variant(const CacheInfoTable &p) {

	Array d;

	for(int i = 0; i < parts.size(); ++i) {
		d.push_back(p.part_holders[p.part_holder_map[parts[i]]]->to_variant());
	}

	Dictionary out;
	out["offset"] = Variant(offset);
	out["total_size"] = Variant(total_size);
	out["guid_prefix"] = Variant(guid_prefix);
	out["parts"] = Variant(d);

	return Variant(out);

}