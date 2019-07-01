#include "cache_info_table.h"

#include "file_cache_manager.h"

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

DescriptorInfo::DescriptorInfo(FileAccess *fa, page_id new_range, int cache_policy) : guid_prefix(new_range), offset(0), valid(true), cache_policy(cache_policy) {
	ERR_FAIL_COND(!fa);
	internal_data_source = fa;
	switch (cache_policy) {
		case FileCacheManager::KEEP:
			max_pages = CS_KEEP_THRESH_DEFAULT;
			break;
		case FileCacheManager::LRU:
			max_pages = CS_LRU_THRESH_DEFAULT;
			break;
		case FileCacheManager::FIFO:
			max_pages = CS_FIFO_THRESH_DEFAULT;
			break;
	}
	total_size = internal_data_source->get_len();
	abs_path = internal_data_source->get_path_absolute();
	sem = Semaphore::create();
	meta_lock = RWLock::create();
	data_lock = RWLock::create();
}

Variant DescriptorInfo::to_variant(const FileCacheManager &p) {

	Dictionary d;

	for(int i = 0; i < pages.size(); ++i) {
		d[itoh(pages[i])] = (p.frames[p.page_frame_map[pages[i]]]->to_variant());
	}

	Dictionary out;
	out["offset"] = Variant(itoh(offset));
	out["total_size"] = Variant(itoh(total_size));
	out["guid_prefix"] = Variant(itoh(guid_prefix));
	out["pages"] = Variant(d);
	out["cache_policy"] = Variant(cache_policy);


	return Variant(out);

}