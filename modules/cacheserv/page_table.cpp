#include "page_table.h"

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

DescriptorInfo::DescriptorInfo(FileAccess *fa, page_id new_range) : range_offset(new_range), offset(0) {
	ERR_FAIL_COND(!fa);
	internal_data_source = fa;
	total_size = internal_data_source->get_len();

}

Variant DescriptorInfo::to_variant(const PageTable &p) {

	Array d;

	for(int i = 0; i < pages.size(); ++i) {
		d.push_back(p.frames[p.page_frame_map[pages[i]]].to_variant());
	}

	Dictionary out;
	out["offset"] = Variant(offset);
	out["total_size"] = Variant(total_size);
	out["range_offset"] = Variant(range_offset);
	out["pages"] = Variant(d);

	return Variant(out);

}