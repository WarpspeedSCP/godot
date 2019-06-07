#include "filecacheserver.h"

#include <unistd.h>

FileCacheServer::FileCacheServer() {
	create_page_table();
	singleton = this;
}

FileCacheServer::~FileCacheServer() {}

FileCacheServer *FileCacheServer::singleton = NULL;
_FileCacheServer *_FileCacheServer::singleton = NULL;

FileCacheServer *FileCacheServer::get_singleton() {
	return singleton;
}

void FileCacheServer::_bind_methods() {}
void _FileCacheServer::_bind_methods() {}

void FileCacheServer::create_page_table() {
	page_table.create();
}

// Allocate a potentially non-contiguous memory region of size 'length'.
size_t FileCacheServer::alloc_in_cache(size_t length) {
	return page_table.allocate(length);
}

void FileCacheServer::free_regions(size_t idx) {
	page_table.free(idx);
}

void FileCacheServer::unlock() {
	if (!thread || !mutex) {
		return;
	}

	mutex->unlock();
}

void FileCacheServer::lock() {
	if (!thread || !mutex) {
		return;
	}

	mutex->lock();
}

Error FileCacheServer::init() {
	exit_thread = false;
	mutex = Mutex::create();
	thread = Thread::create(FileCacheServer::thread_func, this);
	return OK;
}

// Extend an allocation by byte_length bytes.
inline size_t FileCacheServer::extend_alloc_space(size_t start_region_idx, size_t byte_length) {

	size_t curr_region = start_region_idx;
	while (page_table.used_regions[curr_region].next != CS_MEM_VAL_BAD)
		curr_region = page_table.used_regions[curr_region].next;

	size_t extra_regions = alloc_in_cache(byte_length);
	page_table.used_regions[curr_region].next = extra_regions;
	page_table.used_regions[extra_regions].prev = curr_region;

	return 0;
}

// Prepares a contiguous run of pages for usage. Updates the data offset by adding the size of the region.
inline void FileCacheServer::prepare_region(size_t start, size_t size, size_t *data_offset) {
	page_table.prepare_region(start, size, data_offset);
}

void FileCacheServer::thread_func(void *p_udata) {
	FileCacheServer &fcs = *(FileCacheServer *)p_udata;

	// while (!(fcs->exit_thread)) {
	// 	sleep(10);
	// }

	auto a = fcs.alloc_in_cache(CS_PAGE_SIZE * 2);
	auto b = fcs.alloc_in_cache(CS_PAGE_SIZE * 2);
	auto c = fcs.alloc_in_cache(CS_PAGE_SIZE * 2);
	auto d = fcs.alloc_in_cache(CS_PAGE_SIZE * 2);

	auto y = memnew_arr(uint8_t, CS_PAGE_SIZE * 8);

	memset((void *)y, '!', CS_PAGE_SIZE * 2);
	memset((void *)(y + CS_PAGE_SIZE * 2), '*', CS_PAGE_SIZE * 2);
	memset((void *)(y + CS_PAGE_SIZE * 4), '-', CS_PAGE_SIZE * 2);
	memset((void *)(y + CS_PAGE_SIZE * 6), '|', CS_PAGE_SIZE * 2);

	fcs.free_regions(a);
	//fcs->free_regions(b);
	fcs.free_regions(c);
	fcs.free_regions(d);

	auto e = fcs.alloc_in_cache(CS_PAGE_SIZE * 3);
	auto f = fcs.alloc_in_cache(CS_PAGE_SIZE * 2);

	memdelete_arr(y);

	print_line("It woerks");
}

// int FileCacheServer::write_to_regions(void *data, size_t length, size_t start_region) {

// 	size_t offset = 0;
// 	Region curr_region = page_table.used_regions[start_region];

// 	while(true) {

// 		size_t region_size = curr_region.start_page_idx + curr_region.size;

// 		for (size_t i = curr_region.start_page_idx; i < region_size; ++i) {
// 			memcpy(page_table.pages[i].memory_region, (uint8_t *)data + offset, CS_PAGE_SIZE);
// 			page_table.pages.ptrw()[i].data_offset = offset;
// 			page_table.pages.ptrw()[i].recently_used = true;
// 			offset += CS_PAGE_SIZE;
// 		}

// 		if(curr_region.next != CS_MEM_VAL_BAD)
// 			curr_region = page_table.used_regions[curr_region.next];
// 		else break;
// 	}
// 	return 0;
// }

// int FileCacheServer::write_to_single_region(void *data, size_t length, size_t data_offset, size_t region_idx) {
// 	Region curr = page_table.used_regions[region_idx];

// 	memcpy(page_table.pages[curr.start_page_idx].memory_region, data + data_offset, )

// 	return 0;
// }

Vector<Region> FileCacheServer::list_regions(size_t start_idx) {
	Vector<Region> regions;
	size_t idx = start_idx;
	while (idx != CS_MEM_VAL_BAD) {
		Region curr = page_table.used_regions[idx];
		regions.push_back(curr);
		idx = curr.next;
	}

	return regions;
}

_FileCacheServer::_FileCacheServer() {
	singleton = this;
}

_FileCacheServer *_FileCacheServer::get_singleton() {
	return singleton;
}
