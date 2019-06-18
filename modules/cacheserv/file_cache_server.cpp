#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "drivers/unix/file_access_unix.h"
#include "drivers/unix/mutex_posix.h"

#include "file_cache_server.h"
#include "file_access_cached.h"


FileCacheServer::FileCacheServer() {

	files = HashMap<uint32_t, DescriptorInfo *>();
	mutex = NULL;

	page_table.memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);
	page_table.page_frame_map.clear();
	page_table.pages.clear();
	page_table.frames.clear();
	page_table.ranges.clear();

	page_table.available_space = CS_CACHE_SIZE;
	page_table.used_space = 0;
	page_table.total_space = CS_CACHE_SIZE;

	for (size_t i = 0; i < CS_NUM_FRAMES; ++i) {
		page_table.frames.push_back(
				Frame(page_table.memory_region + i * CS_PAGE_SIZE));
	}

	singleton = this;
}

FileCacheServer::~FileCacheServer() {
	if(page_table.memory_region) memdelete(page_table.memory_region);

	if(files.size()) {
		const data_descriptor *key = NULL;
		for(key = files.next(NULL); key; key = files.next(key)) {
			memdelete(files[*key]);
		}
	}

	memdelete(this->mutex);

}

data_descriptor FileCacheServer::add_data_source(RID *rid, FileAccess *data_source) {
	ERR_FAIL_COND_V(!rid->is_valid(), CS_MEM_VAL_BAD);
	data_descriptor dd = rid->get_id();

	size_t new_range;
	while (page_table.ranges.has(new_range = (size_t)random() << 40)) ;

	files[dd] = memnew(DescriptorInfo(data_source, new_range));
	page_table.ranges.insert(new_range);

	for (Set<page_id>::Element *i = page_table.ranges.front(); i; i = i->next())
		printf("\t\t%lx\n", i->get());

	ERR_FAIL_COND_V(files[dd] == NULL, CS_MEM_VAL_BAD);
	seek(rid, 0, SEEK_SET);
	return dd;
}

void FileCacheServer::remove_data_source(data_descriptor dd) {
	if (files.has(dd)) {
		DescriptorInfo *di = files[dd];
		page_table.ranges.erase(di->range_offset);
		for (int i = 0; i < page_table.pages.size(); ++i) {
			if ((page_table.pages[i] & 0xFFFFFF0000000000) == di->range_offset) {
				page_table.pages.set(i, CS_MEM_VAL_BAD);
			}
		}

		page_table.pages.sort();
		for (int i = 0; i < page_table.pages.size(); i++) {
			if (page_table.pages[i] == CS_MEM_VAL_BAD) {
				page_table.pages.resize(i);
				break;
			}
		}

		memdelete(files[dd]);
		files.erase(dd);
	}
}

// !!! takes mutable references to all params.
// This operation selects a new frame, or evicts an old frame to hold a new page.
// TODO: Make this use actual caching algorithms.
void FileCacheServer::do_paging_op(DescriptorInfo *desc_info, page_id curr_page, frame_id *curr_frame, size_t extra_offset) {

	*curr_frame = CS_MEM_VAL_BAD;
	// Find a free frame.
	for (int i = 0; i < CS_NUM_FRAMES; ++i) {
		if (!page_table.frames[i].used) {
			Frame &f = page_table.frames.ptrw()[i];
			MutexLock m = MutexLock(f.m);
				f.used = true;
				f.recently_used = true;
				*curr_frame = i;
				break;
		}
	}

	printf("do_paging_op : curr_frame = %lx\n", curr_frame);

	// If there are no free frames, we evict an old one according to the paging/caching algo (TODO).
	if (*curr_frame == CS_MEM_VAL_BAD) {
		printf("must evict\n");
		// Evict other page somehow...
		// Remove prev page-frame mappings and associated pages.


		page_id page_to_evict = CS_MEM_VAL_BAD;
		frame_id frame_to_evict = CS_MEM_VAL_BAD;
		do {
			page_to_evict = random() % page_table.pages.size();
			page_to_evict = page_table.pages[page_to_evict];
			frame_id frame_to_evict = page_table.page_frame_map[page_to_evict];

		} while(page_table.frames[frame_to_evict].m->try_lock() != OK);

		page_table.frames[frame_to_evict].m->unlock();

		printf("Evicting page %lx mapped to frame %lx\n", page_to_evict, frame_to_evict);

		Frame &f = page_table.frames.ptrw()[frame_to_evict];
		if (f.dirty) {
			enqueue_store(desc_info, CS_GET_FILE_OFFSET_FROM_GUID(curr_page));
		}

		MutexLock m = MutexLock(f.m);
		// Reset flags.
		f.dirty = false;
		f.recently_used = false;
		f.used = false;

		// Erase old info.
		page_table.page_frame_map.erase(page_to_evict);
		page_table.pages.erase(page_to_evict);


		// We reuse the frame we evicted.
		*curr_frame = frame_to_evict;
		printf("do_paging_op : curr_frame = %lx\n", curr_frame);
		f.used = true;
		f.recently_used = true;
	}
}


// !!! takes mutable references to all params.
// Takes an extra_offset parameter that we use to keep track
// of temporary updates to the current offset in the file.
void FileCacheServer::do_load_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t extra_offset) {
	// Get data from data source somehow...

	if (check_incomplete_nonfinal_page_load(desc_info, curr_page, curr_frame, extra_offset)) {
		ERR_PRINT("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
	} else {
		printf("do_load_op : loaded 0x%lx bytes\n", CS_PAGE_SIZE);
	}
}

// !!! takes mutable references to all params.
void FileCacheServer::do_store_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame) {
	// Get data from data source somehow...

	if (check_incomplete_nonfinal_page_store(desc_info, curr_page, curr_frame)) {
		ERR_PRINT("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
	} else {
		printf("do_load_op : loaded 0x%lx bytes\n", CS_PAGE_SIZE);
	}
}


// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A read from the current offset returns less than CS_PAGE_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the frame.
_FORCE_INLINE_ bool FileCacheServer::check_incomplete_nonfinal_page_load(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t extra_offset) {
	MutexLock lock = MutexLock(page_table.frames[curr_frame].m);
	desc_info->internal_data_source->seek(CS_GET_FILE_OFFSET_FROM_GUID(curr_page));
	size_t used_size = page_table.frames.ptrw()[curr_frame].used_size = desc_info->internal_data_source->get_buffer(page_table.frames[curr_frame].memory_region, CS_PAGE_SIZE);
	return (used_size < CS_PAGE_SIZE) && (CS_GET_PAGE(desc_info->offset + extra_offset) < CS_GET_PAGE(desc_info->total_size));
}

// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A write from the current offset returns less than CS_PAGE_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the frame.
_FORCE_INLINE_ bool FileCacheServer::check_incomplete_nonfinal_page_store(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame) {
	MutexLock lock = MutexLock(page_table.frames[curr_frame].m);
	desc_info->internal_data_source->seek(CS_GET_FILE_OFFSET_FROM_GUID(curr_page));
	desc_info->internal_data_source->store_buffer(page_table.frames[curr_frame].memory_region, CS_PAGE_SIZE);
	return desc_info->internal_data_source->get_error() == ERR_FILE_CANT_WRITE && (CS_GET_PAGE(desc_info->offset) < CS_GET_PAGE(desc_info->total_size));
}

// Perform a read operation.
size_t FileCacheServer::read(const RID *const rid, void *const buffer, size_t length) {

	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo &desc_info = **elem;
		size_t final_partial_length = CS_PARTIAL_SIZE(length);
		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;
		size_t buffer_offset = 0;

		// We need to handle the first and last frames differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{
			// Query for the page with the current offset.
			ERR_FAIL_COND_V(curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_table.page_frame_map[curr_page]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			MutexLock m = MutexLock(page_table.frames[curr_frame].m);

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset)
			//  gives us the address of the first byte to copy which may or may not be on a page boundary.
			//
			// We can copy only CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset) which gives us the number
			//  of bytes from the current offset to the end of the page.
			memcpy(
					(uint8_t *)buffer + buffer_offset,
					page_table.frames[curr_frame].memory_region + CS_PARTIAL_SIZE(desc_info.offset),
					CS_PAGE_SIZE - CS_PARTIAL_SIZE(desc_info.offset));

			buffer_offset += CS_PAGE_SIZE - CS_PARTIAL_SIZE(desc_info.offset);
		}

		// Pages in the middle must be copied in full.
		while (buffer_offset < (length - final_partial_length)) {

			ERR_FAIL_COND_V(curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			ERR_FAIL_COND_V((curr_frame = page_table.page_frame_map[curr_page]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock current frame.
			MutexLock m = MutexLock(page_table.frames[curr_frame].m);

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset) gives us the start
			memcpy(
					(uint8_t *)buffer + buffer_offset,
					page_table.frames[curr_frame].memory_region,
					CS_PAGE_SIZE);

			buffer_offset += CS_PAGE_SIZE;
		}

		// For final potentially partially filled page
		if (final_partial_length) {

			ERR_FAIL_COND_V(curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			ERR_FAIL_COND_V((curr_frame = page_table.page_frame_map[curr_page]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			MutexLock m = MutexLock(page_table.frames[curr_frame].m);

			memcpy((uint8_t *)buffer + buffer_offset, page_table.frames[curr_frame].memory_region, final_partial_length);
			buffer_offset += final_partial_length;
		}

		// We update the current offset at the end of the operation.
		desc_info.offset += buffer_offset;

		// Update descriptor info.
		// file_page_map.insert(dd, desc_info);

		return buffer_offset;
	}
}

// Similar to the read operation but opposite data flow.
size_t FileCacheServer::write(const RID *const rid, const void *const data, size_t length) {
	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		size_t final_partial_length = (length % CS_PAGE_SIZE);
		DescriptorInfo &desc_info = **elem;
		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;
		size_t data_offset = 0;

		// Special handling of first page.
		{

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_table.page_frame_map[curr_page]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			MutexLock m = MutexLock(page_table.frames[curr_frame].m);

			// Set the dirty bit.
			page_table.frames.ptrw()[curr_frame].dirty = true;

			memcpy(
					page_table.frames[curr_frame].memory_region + CS_PARTIAL_SIZE(desc_info.offset),
					(uint8_t *)data + data_offset,
					CS_PAGE_SIZE - CS_PARTIAL_SIZE(desc_info.offset));
			// Update offset with number of bytes read in first iteration.
			data_offset += CS_PAGE_SIZE - CS_PARTIAL_SIZE(desc_info.offset);
		}

		while (data_offset < length - final_partial_length) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_table.page_frame_map[curr_page]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			MutexLock m = MutexLock(page_table.frames[curr_frame].m);

			memcpy(
					page_table.frames[curr_frame].memory_region,
					(uint8_t *)data + data_offset,
					CS_PAGE_SIZE);
			data_offset += CS_PAGE_SIZE;
		}

		// For final potentially partially filled page
		if (final_partial_length) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_table.page_frame_map[curr_page]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			MutexLock m = MutexLock(page_table.frames[curr_frame].m);

			memcpy(
					page_table.frames[curr_frame].memory_region,
					(uint8_t *)data + data_offset,
					final_partial_length);
			data_offset += final_partial_length;
		}

		desc_info.offset += data_offset;

		// Update descriptor info.
		// file_page_map.insert(dd, desc_info);

		return data_offset;
	}
}

// The seek operation just uses the POSIX seek modes, which will probably get replaced later.
size_t FileCacheServer::seek(const RID *const rid, size_t new_offset, int mode) {
	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo *desc_info = *elem;
		size_t curr_offset = desc_info->offset;
		size_t end_offset = desc_info->total_size;
		ssize_t eff_offset = 0;
		switch (mode) {
			case SEEK_SET:
				eff_offset += new_offset;
				break;
			case SEEK_CUR:
				eff_offset += curr_offset + new_offset;
				break;
			case SEEK_END:
				eff_offset += end_offset + new_offset;
				break;
			default:
				ERR_PRINT("Invalid mode parameter.")
				return CS_MEM_VAL_BAD;
		}

		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;

		if (eff_offset < 0) {
			ERR_PRINT("Invalid offset.")
			return CS_MEM_VAL_BAD;

		// In this case, there can only be a hole and no data, so we only
		// do a paging operation to represent it in memory, in case of a write.
		} else if (eff_offset > end_offset) {
			for(int i = 0; i < CS_SEEK_READ_AHEAD_SIZE; i++) {
				check_with_page_op(desc_info, eff_offset + i * CS_PAGE_SIZE);
			}

		} else {
			for(int i = 0; i < CS_SEEK_READ_AHEAD_SIZE; i++) {
				check_with_page_op_and_update(desc_info, &curr_page, &curr_frame, eff_offset + i * CS_PAGE_SIZE);
				enqueue_load(desc_info, curr_page);
			}
		}

		// Update the offset.

		desc_info->offset = eff_offset;

		// Update descriptor info.
		// file_page_map.insert(dd, desc_info);

		return curr_offset;
	}
}

size_t FileCacheServer::get_len(const RID *const rid) const {
	data_descriptor dd = rid->get_id();
	size_t size = files[dd]->internal_data_source->get_len();
	if(size > files[dd]->total_size) {
		files[dd]->total_size = size;
	}

	return size;
}

bool FileCacheServer::file_exists(const String &p_name) const {
	FileAccess *f = FileAccess::create(FileAccess::ACCESS_FILESYSTEM);
	bool exists = f->file_exists(p_name);
	memdelete(f);
	return exists;

}

bool FileCacheServer::eof_reached(const RID *const rid) const {
	bool eof = files[rid->get_id()]->internal_data_source->eof_reached();
	return eof;
}

bool FileCacheServer::check_with_page_op(DescriptorInfo *desc_info, size_t offset) {

	if (get_page_guid(*desc_info, offset, true) == CS_MEM_VAL_BAD) {

		page_id cp = get_page_guid(*desc_info, offset, false);
		frame_id cf = CS_MEM_VAL_BAD;

		desc_info->pages.ordered_insert(cp);
		page_table.pages.ordered_insert(cp);

		do_paging_op(desc_info, cp, &cf);
		ERR_FAIL_COND_V(cf == CS_MEM_VAL_BAD, false);

		page_table.page_frame_map.insert(cp, cf);

		return false;
	}

	return true;
}


bool FileCacheServer::check_with_page_op_and_update(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame, size_t offset) {

	if (get_page_guid(*desc_info, offset, true) == CS_MEM_VAL_BAD) {

		page_id cp = get_page_guid(*desc_info, offset, false);
		frame_id cf = CS_MEM_VAL_BAD;

		desc_info->pages.ordered_insert(cp);
		page_table.pages.ordered_insert(cp);

		do_paging_op(desc_info, cp, &cf);
		ERR_FAIL_COND_V(cf == CS_MEM_VAL_BAD, false);

		page_table.page_frame_map.insert(cp, cf);

		if(curr_frame)
			*curr_frame = cf;
		if(curr_page)
			*curr_page = cp;

		return false;
	}

	return true;
}

FileCacheServer *FileCacheServer::singleton = NULL;
_FileCacheServer *_FileCacheServer::singleton = NULL;

FileCacheServer *FileCacheServer::get_singleton() {
	return singleton;
}

// void FileCacheServer::_bind_methods() {
// 	ClassDB::bind_method(D_METHOD("_get_state"), &FileCacheServer::_get_state);
// }
// void _FileCacheServer::_bind_methods() {}

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
	thread = Thread::create(FileCacheServer::thread_func, this);

	th2 = Thread::create(FileCacheServer::th2_fn, this);

	return OK;
}

#define DBG_PRINT //	printf("\n\n");\
// 	for(auto i = p.page_frame_map.front(); i; i = i->next()) \
// 		printf("%lx : %lx\n", i->key(), i->value()); \
// 	printf("\n\n");


void FileCacheServer::thread_func(void *p_udata) {
	srandom(time(0));
	FileCacheServer &fcs = *static_cast<FileCacheServer *>(p_udata);
	fcs.mutex = Mutex::create();

	// FileAccess::make_default<FileAccessUnbuffered>(FileAccess::ACCESS_FILESYSTEM);

	// while (!(fcs->exit_thread)) {
	// 	sleep(10);
	// }

	// FileAccess * r = memnew(FileAccessCached);

	// PageTable &p = fcs.page_table;
	// FileAccessUnix *f, *g;
	// f = memnew(FileAccessUnix);
	// g = f;
	// f = (FileAccessUnix *)f->open(String("nbig.txt"), FileAccess::READ_WRITE);
	// memdelete(g);

	while(!fcs.exit_thread) {

		CtrlOp l = fcs.op_queue.pop();
		printf("got message\n");
		ERR_CONTINUE(l.di == NULL);

		page_id curr_page = CS_GET_PAGE(l.offset);
		frame_id curr_frame = fcs.page_table.page_frame_map[CS_GET_PAGE(l.offset)];
		MutexLock frame_lock = MutexLock(fcs.page_table.frames[curr_frame].m);

		switch (l.type) {
			case CtrlOp::LOAD: {
				fcs.do_load_op(l.di, curr_page, curr_frame);
				break;
			}
			case CtrlOp::STORE: {
				fcs.do_store_op(l.di, curr_page, curr_frame);
				break;
			}
			default: ERR_FAIL();
		}
	}
}

void FileCacheServer::th2_fn(void *p_udata) {
		sleep(2);

		FileCacheServer &fcs = *static_cast<FileCacheServer *>(p_udata);

		FileAccessCached *fac = memnew(FileAccessCached);
		fac->_open("/home/warpspeedscp/godot/big.txt", FileAccess::READ);

		while (!fcs.exit_thread);

		fac->close();
	}

void FileCacheServer::check_cache(const RID *const rid, size_t length) {
	DescriptorInfo *desc_info = files[rid->get_id()];


	for(int i = 0; i < CS_GET_PAGED_LENGTH(length); ++i) {
		if(!check_with_page_op(desc_info, desc_info->offset + i * CS_PAGE_SIZE)) {
			enqueue_load(desc_info, desc_info->offset + i * CS_PAGE_SIZE);
		}
	}
}

_FileCacheServer::_FileCacheServer() {
	singleton = this;
}

_FileCacheServer *_FileCacheServer::get_singleton() {
	return singleton;
}
