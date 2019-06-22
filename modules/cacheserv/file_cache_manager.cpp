/*************************************************************************/
/*  file_cache_manager.cpp                                               */
/*************************************************************************/
/*                       This file is part of:                           */
/*                           GODOT ENGINE                                */
/*                      https://godotengine.org                          */
/*************************************************************************/
/* Copyright (c) 2007-2019 Juan Linietsky, Ariel Manzur.                 */
/* Copyright (c) 2014-2019 Godot Engine contributors (cf. AUTHORS.md)    */
/*                                                                       */
/* Permission is hereby granted, free of charge, to any person obtaining */
/* a copy of this software and associated documentation files (the       */
/* "Software"), to deal in the Software without restriction, including   */
/* without limitation the rights to use, copy, modify, merge, publish,   */
/* distribute, sublicense, and/or sell copies of the Software, and to    */
/* permit persons to whom the Software is furnished to do so, subject to */
/* the following conditions:                                             */
/*                                                                       */
/* The above copyright notice and this permission notice shall be        */
/* included in all copies or substantial portions of the Software.       */
/*                                                                       */
/* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       */
/* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    */
/* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*/
/* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY  */
/* CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,  */
/* TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE     */
/* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.                */
/*************************************************************************/

#include "file_cache_manager.h"
#include "file_access_cached.h"

#include "drivers/unix/file_access_unix.h"
#include "drivers/unix/mutex_posix.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>

static const bool CS_TRUE = true;
static const bool CS_FALSE = true;

FileCacheManager::FileCacheManager() {

	files = HashMap<uint32_t, DescriptorInfo *>();
	mutex = Mutex::create();

	cache_info_table.memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);
	cache_info_table.page_frame_map.clear();
	cache_info_table.parts.clear();
	cache_info_table.part_holders.clear();
	cache_info_table.guid_prefixes.clear();

	cache_info_table.available_space = CS_CACHE_SIZE;
	cache_info_table.used_space = 0;
	cache_info_table.total_space = CS_CACHE_SIZE;

	for (size_t i = 0; i < CS_NUM_PART_HOLDERS; ++i) {
		cache_info_table.part_holders.push_back(
				memnew(PartHolder(cache_info_table.memory_region + i * CS_PART_SIZE)));
	}

	singleton = this;
}

FileCacheManager::~FileCacheManager() {
	WARN_PRINT("Destructor running.");
	if (cache_info_table.memory_region) memdelete(cache_info_table.memory_region);

	if (files.size()) {
		const data_descriptor *key = NULL;
		for (key = files.next(NULL); key; key = files.next(key)) {
			memdelete(files[*key]);
		}
	}

	for(int i = 0; i < cache_info_table.part_holders.size(); ++i) {
		memdelete(cache_info_table.part_holders[i]);
	}

	this->op_queue.sig_quit = true;
	this->op_queue.push(CtrlOp());
	this->exit_thread = true;
	Thread::wait_to_finish(this->thread);
	//Thread::wait_to_finish(this->th2);
	memdelete(thread);
	//memdelete(th2);
	memdelete(this->mutex);
}

data_descriptor FileCacheManager::add_data_source(RID *rid, FileAccess *data_source) {
	ERR_FAIL_COND_V(!rid->is_valid(), CS_MEM_VAL_BAD);
	data_descriptor dd = rid->get_id();

	size_t new_guid_prefix;
	while (cache_info_table.guid_prefixes.has(new_guid_prefix = (size_t)random() << 40))
		;

	files[dd] = memnew(DescriptorInfo(data_source, new_guid_prefix));
	cache_info_table.guid_prefixes.insert(new_guid_prefix);

	for (Set<part_id>::Element *i = cache_info_table.guid_prefixes.front(); i; i = i->next())
		printf("\t\t%lx\n", i->get());

	ERR_FAIL_COND_V(files[dd] == NULL, CS_MEM_VAL_BAD);
	seek(rid, 0, SEEK_SET);
	return dd;
}

void FileCacheManager::remove_data_source(data_descriptor dd) {
	if (files.has(dd)) {
		DescriptorInfo *di = files[dd];
		cache_info_table.guid_prefixes.erase(di->guid_prefix);
		for (int i = 0; i < cache_info_table.parts.size(); ++i) {
			if ((cache_info_table.parts[i] & 0xFFFFFF0000000000) == di->guid_prefix) {
				cache_info_table.parts.set(i, CS_MEM_VAL_BAD);
			}
		}

		cache_info_table.parts.sort();
		for (int i = 0; i < cache_info_table.parts.size(); i++) {
			if (cache_info_table.parts[i] == CS_MEM_VAL_BAD) {
				cache_info_table.parts.resize(i);
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
void FileCacheManager::do_paging_op(DescriptorInfo *desc_info, part_id curr_part, part_holder_id *curr_part_holder, size_t extra_offset) {

	*curr_part_holder = CS_MEM_VAL_BAD;
	// Find a free frame.
	for (int i = 0; i < CS_NUM_PART_HOLDERS; ++i) {
		PartHolder::Write w(cache_info_table.part_holders[i]);

		if (!w.get_used()) {
			w.set_used(true);
			w.set_recently_used(true);
			*curr_part_holder = i;
			break;
		}
	}

	printf("do_paging_op : curr_part_holder = %lx\n", curr_part_holder);

	// If there are no free part_holders, we evict an old one according to the paging/caching algo (TODO).
	if (*curr_part_holder == CS_MEM_VAL_BAD) {
		printf("must evict\n");
		// Evict other page somehow...
		// Remove prev page-frame mappings and associated parts.

		part_id page_to_evict = CS_MEM_VAL_BAD;
		part_holder_id frame_to_evict = CS_MEM_VAL_BAD;

		{
			PartHolder::Write w;
			//TODO : change as per proper cache algo.
			do {
				page_to_evict = random() % cache_info_table.parts.size();
				page_to_evict = cache_info_table.parts[page_to_evict];
				part_holder_id frame_to_evict = cache_info_table.page_frame_map[page_to_evict];
				w = cache_info_table.part_holders[frame_to_evict];
			} while (!w.is_valid());
		}

		printf("Evicting page %lx mapped to frame %lx\n", page_to_evict, frame_to_evict);

		PartHolder::Write w(cache_info_table.part_holders[frame_to_evict]);
		if (w.get_dirty()) {
			enqueue_store(desc_info, CS_GET_FILE_OFFSET_FROM_GUID(curr_part));
		}

		// Reset flags.
		w.set_dirty(false);
		w.set_recently_used(false);
		w.set_used(false);

		// Erase old info.
		cache_info_table.page_frame_map.erase(page_to_evict);
		cache_info_table.parts.erase(page_to_evict);

		// We reuse the frame we evicted.
		*curr_part_holder = frame_to_evict;
		printf("do_paging_op : curr_part_holder = %lx\n", curr_part_holder);
		w.set_used(true);
		w.set_recently_used(true);
	}
}

// !!! takes mutable references to all params.
// Takes an extra_offset parameter that we use to keep track
// of temporary updates to the current offset in the file.
void FileCacheManager::do_load_op(DescriptorInfo *desc_info, part_id curr_part, part_holder_id curr_part_holder, size_t extra_offset) {
	// Get data from data source somehow...

	if (check_incomplete_nonfinal_page_load(desc_info, curr_part, curr_part_holder, extra_offset)) {
		ERR_PRINT("Read less than " STRINGIFY(CS_PART_SIZE) " bytes.")
	} else {
		printf("do_load_op : loaded 0x%lx bytes\n", CS_PART_SIZE);
	}
}

// !!! takes mutable references to all params.
void FileCacheManager::do_store_op(DescriptorInfo *desc_info, part_id curr_part, part_holder_id curr_part_holder) {
	// store back to data source somehow...

	if(!PartHolder::Read(cache_info_table.part_holders[curr_part_holder]).get_dirty()){
		WARN_PRINT("Nothing to write back.");
		return;
	}

	if (check_incomplete_nonfinal_page_store(desc_info, curr_part, curr_part_holder)) {
		ERR_PRINT("Read less than " STRINGIFY(CS_PART_SIZE) " bytes.")
	} else {
		printf("do_store_op : loaded 0x%lx bytes\n", CS_PART_SIZE);
	}
}

// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A read from the current offset returns less than CS_PART_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the frame.
_FORCE_INLINE_ bool FileCacheManager::check_incomplete_nonfinal_page_load(DescriptorInfo *desc_info, part_id curr_part, part_holder_id curr_part_holder, size_t extra_offset) {
	PartHolder::Write w(cache_info_table.part_holders[curr_part_holder]);
	desc_info->internal_data_source->seek(CS_GET_FILE_OFFSET_FROM_GUID(curr_part));
	size_t used_size = desc_info->internal_data_source->get_buffer(w.ptr(), CS_PART_SIZE);
	String s = String((char *)w.ptr());
	s.resize(used_size % 100);
	WARN_PRINT(("First 100 or less bytes: " + s).utf8().get_data());
	w.set_used_size(used_size);
	return (used_size < CS_PART_SIZE) && (CS_GET_PART(desc_info->offset + extra_offset) < CS_GET_PART(desc_info->total_size));
}

// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A write from the current offset returns less than CS_PART_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the frame.
_FORCE_INLINE_ bool FileCacheManager::check_incomplete_nonfinal_page_store(DescriptorInfo *desc_info, part_id curr_part, part_holder_id curr_part_holder) {
	PartHolder::Read r(cache_info_table.part_holders[curr_part_holder]);
	desc_info->internal_data_source->seek(CS_GET_FILE_OFFSET_FROM_GUID(curr_part));
	desc_info->internal_data_source->store_buffer(r.ptr(), CS_PART_SIZE);
	return desc_info->internal_data_source->get_error() == ERR_FILE_CANT_WRITE && (CS_GET_PART(desc_info->offset) < CS_GET_PART(desc_info->total_size));
}

// Perform a read operation.
size_t FileCacheManager::read(const RID *const rid, void *const buffer, size_t length) {

	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo &desc_info = **elem;
		size_t final_partial_length = CS_PARTIAL_SIZE(length);
		part_id curr_part = CS_MEM_VAL_BAD;
		part_holder_id curr_part_holder = CS_MEM_VAL_BAD;
		size_t buffer_offset = 0;

		// We need to handle the first and last part_holders differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{
			// Query for the page with the current offset.
			ERR_FAIL_COND_V(curr_part = get_part_guid(desc_info, desc_info.offset + buffer_offset, true) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_part_holder = cache_info_table.page_frame_map[curr_part]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			PartHolder::Read r(cache_info_table.part_holders[curr_part_holder]);

			// Here, part_holders[curr_part_holder].memory_region + PARTIAL_SIZE(desc_info.offset)
			//  gives us the address of the first byte to copy which may or may not be on a page boundary.
			//
			// We can copy only CS_PART_SIZE - PARTIAL_SIZE(desc_info.offset) which gives us the number
			//  of bytes from the current offset to the end of the page.
			memcpy(
					(uint8_t *)buffer + buffer_offset,
					r.ptr() + CS_PARTIAL_SIZE(desc_info.offset),
					CS_PART_SIZE - CS_PARTIAL_SIZE(desc_info.offset));

			buffer_offset += CS_PART_SIZE - CS_PARTIAL_SIZE(desc_info.offset);
		}

		// Pages in the middle must be copied in full.
		while (buffer_offset < (length - final_partial_length)) {

			ERR_FAIL_COND_V(curr_part = get_part_guid(desc_info, desc_info.offset + buffer_offset, true) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			ERR_FAIL_COND_V((curr_part_holder = cache_info_table.page_frame_map[curr_part]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock current frame.
			PartHolder::Read r(cache_info_table.part_holders[curr_part_holder]);

			// Here, part_holders[curr_part_holder].memory_region + PARTIAL_SIZE(desc_info.offset) gives us the start
			memcpy(
					(uint8_t *)buffer + buffer_offset,
					r.ptr(),
					CS_PART_SIZE);

			buffer_offset += CS_PART_SIZE;
		}

		// For final potentially partially filled page
		if (final_partial_length) {

			ERR_FAIL_COND_V(curr_part = get_part_guid(desc_info, desc_info.offset + buffer_offset, true) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			ERR_FAIL_COND_V((curr_part_holder = cache_info_table.page_frame_map[curr_part]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			PartHolder::Read r(cache_info_table.part_holders[curr_part_holder]);

			memcpy(
					(uint8_t *)buffer + buffer_offset,
					r.ptr(),
					final_partial_length);
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
size_t FileCacheManager::write(const RID *const rid, const void *const data, size_t length) {
	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		size_t final_partial_length = (length % CS_PART_SIZE);
		DescriptorInfo &desc_info = **elem;
		part_id curr_part = CS_MEM_VAL_BAD;
		part_holder_id curr_part_holder = CS_MEM_VAL_BAD;
		size_t data_offset = 0;

		// Special handling of first page.
		{

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_part = get_part_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_part_holder = cache_info_table.page_frame_map[curr_part]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			PartHolder::Write w(cache_info_table.part_holders[curr_part_holder]);

			// Set the dirty bit.
			w.set_dirty(true);

			memcpy(
					w.ptr() + CS_PARTIAL_SIZE(desc_info.offset),
					(uint8_t *)data + data_offset,
					CS_PART_SIZE - CS_PARTIAL_SIZE(desc_info.offset));
			// Update offset with number of bytes read in first iteration.
			data_offset += CS_PART_SIZE - CS_PARTIAL_SIZE(desc_info.offset);
		}

		while (data_offset < length - final_partial_length) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_part = get_part_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_part_holder = cache_info_table.page_frame_map[curr_part]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			PartHolder::Write w(cache_info_table.part_holders[curr_part_holder]);

			// Set the dirty bit.
			w.set_dirty(true);

			memcpy(
					w.ptr(),
					(uint8_t *)data + data_offset,
					CS_PART_SIZE);
			data_offset += CS_PART_SIZE;
		}

		// For final potentially partially filled page
		if (final_partial_length) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_part = get_part_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			// Get frame mapped to page.
			ERR_FAIL_COND_V((curr_part_holder = cache_info_table.page_frame_map[curr_part]) != CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Lock the frame for the operation.
			PartHolder::Write w(cache_info_table.part_holders[curr_part_holder]);

			// Set the dirty bit.
			w.set_dirty(true);

			memcpy(
					w.ptr(),
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
size_t FileCacheManager::seek(const RID *const rid, size_t new_offset, int mode) {
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

		part_id curr_part = CS_MEM_VAL_BAD;
		part_holder_id curr_part_holder = CS_MEM_VAL_BAD;

		if (eff_offset < 0) {
			ERR_PRINT("Invalid offset.")
			return CS_MEM_VAL_BAD;

			// In this case, there can only be a hole and no data, so we only
			// do a paging operation to represent it in memory, in case of a write.
		} else if (eff_offset > end_offset) {
			for (int i = 0; i < CS_SEEK_READ_AHEAD_SIZE; i++) {
				check_with_page_op(desc_info, eff_offset + i * CS_PART_SIZE);
			}

		} else {
			for (int i = 0; i < CS_SEEK_READ_AHEAD_SIZE; i++) {
				check_with_page_op_and_update(desc_info, &curr_part, &curr_part_holder, eff_offset + i * CS_PART_SIZE);
				enqueue_load(desc_info, curr_part);
			}
		}

		// Update the offset.

		desc_info->offset = eff_offset;

		// Update descriptor info.
		// file_page_map.insert(dd, desc_info);

		return curr_offset;
	}
}

size_t FileCacheManager::get_len(const RID *const rid) const {
	data_descriptor dd = rid->get_id();
	size_t size = files[dd]->internal_data_source->get_len();
	if (size > files[dd]->total_size) {
		files[dd]->total_size = size;
	}

	return size;
}

bool FileCacheManager::file_exists(const String &p_name) const {
	FileAccess *f = FileAccess::create(FileAccess::ACCESS_FILESYSTEM);
	bool exists = f->file_exists(p_name);
	memdelete(f);
	return exists;
}

bool FileCacheManager::eof_reached(const RID *const rid) const {
	bool eof = files[rid->get_id()]->internal_data_source->eof_reached();
	return eof;
}

bool FileCacheManager::check_with_page_op(DescriptorInfo *desc_info, size_t offset) {

	if (get_part_guid(*desc_info, offset, true) == CS_MEM_VAL_BAD) {

		part_id cp = get_part_guid(*desc_info, offset, false);
		part_holder_id cf = CS_MEM_VAL_BAD;

		desc_info->parts.ordered_insert(cp);
		cache_info_table.parts.ordered_insert(cp);

		do_paging_op(desc_info, cp, &cf);
		ERR_FAIL_COND_V(cf == CS_MEM_VAL_BAD, false);

		cache_info_table.page_frame_map.insert(cp, cf);

		return false;
	}

	return true;
}

bool FileCacheManager::check_with_page_op_and_update(DescriptorInfo *desc_info, part_id *curr_part, part_holder_id *curr_part_holder, size_t offset) {

	if (get_part_guid(*desc_info, offset, true) == CS_MEM_VAL_BAD) {

		part_id cp = get_part_guid(*desc_info, offset, false);
		part_holder_id cf = CS_MEM_VAL_BAD;

		desc_info->parts.ordered_insert(cp);
		cache_info_table.parts.ordered_insert(cp);

		do_paging_op(desc_info, cp, &cf);
		ERR_FAIL_COND_V(cf == CS_MEM_VAL_BAD, false);

		cache_info_table.page_frame_map.insert(cp, cf);

		if (curr_part_holder)
			*curr_part_holder = cf;
		if (curr_part)
			*curr_part = cp;

		return false;
	}

	return true;
}

FileCacheManager *FileCacheManager::singleton = NULL;
_FileCacheManager *_FileCacheManager::singleton = NULL;

FileCacheManager *FileCacheManager::get_singleton() {
	return singleton;
}

// void FileCacheManager::_bind_methods() {
// 	ClassDB::bind_method(D_METHOD("_get_state"), &FileCacheManager::_get_state);
// }
// void _FileCacheManager::_bind_methods() {}

void FileCacheManager::unlock() {
	if (!thread || !mutex) {
		return;
	}

	mutex->unlock();
}

void FileCacheManager::lock() {
	if (!thread || !mutex) {
		return;
	}

	mutex->lock();
}

Error FileCacheManager::init() {
	exit_thread = false;
	thread = Thread::create(FileCacheManager::thread_func, this);

	// th2 = Thread::create(FileCacheManager::th2_fn, this);



	return OK;
}

#define DBG_PRINT //	printf("\n\n");\
// 	for(auto i = p.page_frame_map.front(); i; i = i->next()) \
// 		printf("%lx : %lx\n", i->key(), i->value()); \
// 	printf("\n\n");

void FileCacheManager::thread_func(void *p_udata) {
	srandom(time(0));
	FileCacheManager &fcs = *static_cast<FileCacheManager *>(p_udata);

	// FileAccess::make_default<FileAccessUnbuffered>(FileAccess::ACCESS_FILESYSTEM);

	// while (!(fcs->exit_thread)) {
	// 	sleep(10);
	// }

	// FileAccess * r = memnew(FileAccessCached);

	// CacheInfoTable &p = fcs.cache_info_table;
	// FileAccessUnix *f, *g;
	// f = memnew(FileAccessUnix);
	// g = f;
	// f = (FileAccessUnix *)f->open(String("nbig.txt"), FileAccess::READ_WRITE);
	// memdelete(g);

	do {

//		WARN_PRINT(("Thread" + itos(fcs.thread->get_id())  +   "Waiting for message.").utf8().get_data());
		CtrlOp l = fcs.op_queue.pop();
		WARN_PRINT("got message");
		ERR_CONTINUE(l.di == NULL);

		part_id curr_part = CS_GET_PART(l.offset);
		part_holder_id curr_part_holder = fcs.cache_info_table.page_frame_map[CS_GET_PART(l.offset)];

		switch (l.type) {
			case CtrlOp::LOAD: {
				WARN_PRINT("Performing load.");
				fcs.do_load_op(l.di, curr_part, curr_part_holder);
				break;
			}
			case CtrlOp::STORE: {
				WARN_PRINT("Performing store.");
				fcs.do_store_op(l.di, curr_part, curr_part_holder);
				break;
			}
			default: ERR_FAIL();
		}
	} while (!fcs.exit_thread);
}

void FileCacheManager::check_cache(const RID *const rid, size_t length) {
	DescriptorInfo *desc_info = files[rid->get_id()];

	for (int i = desc_info->offset; i < desc_info->offset + length; i += CS_PART_SIZE) {
		if (!check_with_page_op(desc_info, i)) {
			enqueue_load(desc_info, i);
		}
	}
}

_FileCacheManager::_FileCacheManager() {
	singleton = this;
}

_FileCacheManager *_FileCacheManager::get_singleton() {
	return singleton;
}
