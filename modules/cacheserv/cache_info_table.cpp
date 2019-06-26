#include "cache_info_table.h"

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

DescriptorInfo::DescriptorInfo(FileAccess *fa, page_id new_range) : guid_prefix(new_range), offset(0) {
	ERR_FAIL_COND(!fa);
	internal_data_source = fa;
	total_size = internal_data_source->get_len();
	sem = Semaphore::create();
	meta_lock = RWLock::create();
	data_lock = RWLock::create();
}

Variant DescriptorInfo::to_variant(const CacheInfoTable &p) {

	Dictionary d;

	for(int i = 0; i < pages.size(); ++i) {
		d[itos(pages[i])] = (p.frames[p.page_frame_map[pages[i]]]->to_variant());
	}

	Dictionary out;
	out["offset"] = Variant(offset);
	out["total_size"] = Variant(total_size);
	out["guid_prefix"] = Variant(guid_prefix);
	out["pages"] = Variant(d);

	return Variant(out);

}