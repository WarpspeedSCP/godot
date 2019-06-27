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

#include "core/os/os.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>

static const bool CS_TRUE = true;
static const bool CS_FALSE = true;

FileCacheManager::FileCacheManager() {
	mutex = Mutex::create();
	rng.set_seed(OS::get_singleton()->get_ticks_usec());

	cache_info_table.memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);
	cache_info_table.page_frame_map.clear();
	cache_info_table.pages.clear();
	cache_info_table.frames.clear();
	cache_info_table.guid_prefixes.clear();

	cache_info_table.available_space = CS_CACHE_SIZE;
	cache_info_table.used_space = 0;
	cache_info_table.total_space = CS_CACHE_SIZE;

	for (size_t i = 0; i < CS_NUM_FRAMES; ++i) {
		cache_info_table.frames.push_back(
				memnew(Frame(cache_info_table.memory_region + i * CS_PAGE_SIZE)));
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

	for (int i = 0; i < cache_info_table.frames.size(); ++i) {
		memdelete(cache_info_table.frames[i]);
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

	for (Set<page_id>::Element *i = cache_info_table.guid_prefixes.front(); i; i = i->next())
		printf("\t\t%lx\n", i->get());

	ERR_FAIL_COND_V(files[dd] == NULL, CS_MEM_VAL_BAD);
	seek(rid, 0, SEEK_SET);
	return dd;
}

void FileCacheManager::remove_data_source(data_descriptor dd) {
	if (files.has(dd)) {
		DescriptorInfo *di = files[dd];
		cache_info_table.guid_prefixes.erase(di->guid_prefix);
		for (int i = 0; i < cache_info_table.pages.size(); ++i) {
			if ((cache_info_table.pages[i] & 0xFFFFFF0000000000) == di->guid_prefix) {
				Frame::MetaWrite(cache_info_table.frames[cache_info_table.page_frame_map[di->pages[i]]], di->meta_lock).set_used(false).set_ready_false();
				memset(Frame::DataWrite(cache_info_table.frames[cache_info_table.page_frame_map[di->pages[i]]], di->data_lock).ptr(), 0, 4096);
				cache_info_table.pages.set(i, CS_MEM_VAL_BAD);
			}
		}

		cache_info_table.pages.sort();
		for (int i = 0; i < cache_info_table.pages.size(); i++) {
			if (cache_info_table.pages[i] == CS_MEM_VAL_BAD) {
				cache_info_table.pages.resize(i);
				break;
			}
		}

		memdelete(files[dd]);
		files.erase(dd);
	}
}

void FileCacheManager::enforce_cache_policy(DescriptorInfo *desc_info, page_id curr_page) {
}

// !!! takes mutable references to all params.
// This operation selects a new page holder, or evicts an old page holder to hold a new page.
// TODO: Make this use actual caching algorithms.
void FileCacheManager::do_paging_op(DescriptorInfo *desc_info, page_id curr_page, frame_id *curr_frame, size_t offset) {

	*curr_frame = CS_MEM_VAL_BAD;
	// Find a free page holder.
	for (int i = 0; i < CS_NUM_FRAMES; ++i) {

		if (!cache_info_table.frames[i]->used) {
			Frame::MetaWrite(
					cache_info_table.frames[i], desc_info->meta_lock)
					.set_used(true)
					.set_last_use(0)
					.set_ready_false();
			*curr_frame = i;
			break;
		}
	}

	printf("do_paging_op : curr_frame = %lx\n", *curr_frame);

	// If there are no free frames, we evict an old one according to the paging/caching algo (TODO).
	if (*curr_frame == CS_MEM_VAL_BAD) {
		printf("must evict\n");
		// Evict other page somehow...
		// Remove prev page-page holder mappings and associated pages.

		page_id page_to_evict = CS_MEM_VAL_BAD;
		frame_id frame_to_evict = CS_MEM_VAL_BAD;

		{
			page_to_evict = cache_policies[desc_info->cache_policy](this);

			//TODO : change as per proper cache algo.
			page_to_evict = rng.randi_range(0, cache_info_table.pages.size());
			page_to_evict = cache_info_table.pages[page_to_evict];
			frame_to_evict = cache_info_table.page_frame_map[page_to_evict];
		}

		printf("Evicting page %lx mapped to page holder %lx\n", page_to_evict, frame_to_evict);

		Frame::MetaWrite w(cache_info_table.frames[frame_to_evict], desc_info->meta_lock);
		if (w.get_dirty()) {
			enqueue_store(desc_info, CS_GET_FILE_OFFSET_FROM_GUID(curr_page));
		}

		// Reset flags.
		w.set_dirty(false).set_recently_used(false).set_used(false);

		// Erase old info.
		cache_info_table.page_frame_map.erase(page_to_evict);
		cache_info_table.pages.erase(page_to_evict);

		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;
		printf("do_paging_op : curr_frame = %lx\n", curr_frame);
		w.set_used(true).set_recently_used(true).set_ready_false();
	}
}

// !!! takes mutable references to all params.
// Takes an extra_offset parameter that we use to keep track
// of temporary updates to the current offset in the file.
void FileCacheManager::do_load_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	// Get data from data source somehow...

	if (check_incomplete_page_load(desc_info, curr_page, curr_frame, offset)) {
		ERR_PRINT("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
	} else {
		printf("do_load_op : loaded 0x%lx bytes\n", this->cache_info_table.frames[curr_frame]->used_size);
	}
}

// !!! takes mutable references to all params.
void FileCacheManager::do_store_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	// store back to data source somehow...

	if (!Frame::MetaRead(cache_info_table.frames[curr_frame], desc_info->meta_lock).get_dirty()) {
		WARN_PRINT("Nothing to write back.");
		return;
	}

	if (check_incomplete_page_store(desc_info, curr_page, curr_frame, offset)) {
		ERR_PRINT("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
	} else {
		printf("do_store_op : loaded 0x%lx bytes\n", CS_PAGE_SIZE);
	}
}

// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A read from the current offset returns less than CS_PAGE_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the page holder.
_FORCE_INLINE_ bool FileCacheManager::check_incomplete_page_load(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	desc_info->internal_data_source->seek(offset);
	size_t used_size;
	String s;
	{
		Frame::DataWrite w(cache_info_table.frames[curr_frame], desc_info->data_lock);
		used_size = desc_info->internal_data_source->get_buffer(w.ptr(), CS_PAGE_SIZE);
		s = String((char *)w.ptr()) + " ";
		Frame::MetaWrite(cache_info_table.frames[curr_frame], desc_info->meta_lock).set_used_size(used_size).set_ready_true(desc_info->sem);
	}

	s.resize(used_size % 100);
	WARN_PRINT(("First 100 or less bytes: " + s).utf8().get_data());
	return (used_size < CS_PAGE_SIZE);
}

// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A write from the current offset returns less than CS_PAGE_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the page holder.
_FORCE_INLINE_ bool FileCacheManager::check_incomplete_page_store(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	desc_info->internal_data_source->seek(offset);
	{
		Frame::DataRead r(cache_info_table.frames[curr_frame], desc_info->sem, desc_info->data_lock);
		desc_info->internal_data_source->store_buffer(r.ptr(), CS_PAGE_SIZE);
		Frame::MetaWrite(cache_info_table.frames[curr_frame], desc_info->meta_lock).set_dirty(false);
	}
	return desc_info->internal_data_source->get_error() == ERR_FILE_CANT_WRITE;
}

// Perform a read operation.
size_t FileCacheManager::read(const RID *const rid, void *const buffer, size_t length) {

	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo &desc_info = **elem;
		size_t initial_start_offset = desc_info.offset;
		size_t initial_end_offset = initial_start_offset + CS_PAGE_SIZE;
		size_t final_partial_length = CS_PARTIAL_SIZE(initial_start_offset + length);
		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;
		size_t buffer_offset = 0;

		// We need to handle the first and last frames differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{
			// Query for the page with the current offset.

			//ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD)
			{
				if (unlikely((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == (size_t)~0)) {
					_err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Condition ' " _STR((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == (size_t)~0) " ' is true. returned: " _STR((size_t)~0));
					return (size_t)~0;
				} else
					_err_error_exists = false;
			}
			// Get page holder mapped to page.

			//ERR_FAIL_COND_V((curr_frame = cache_info_table.page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			{
				if (unlikely((curr_frame = cache_info_table.page_frame_map[curr_page]) == (size_t)~0)) {
					_err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Condition ' " _STR((curr_frame = cache_info_table.page_frame_map[curr_page]) == (size_t)~0) " ' is true. returned: " _STR((size_t)~0));
					return (size_t)~0;
				} else
					_err_error_exists = false;
			}

			// The end offset of the first page may not be greater than the start offset of the next page.
			initial_end_offset = CLAMP(initial_start_offset + length, 0, CS_GET_PAGE(initial_start_offset) + CS_PAGE_SIZE);

			WARN_PRINT("Reading first page.");

			{ // Lock the page holder for the operation.
				Frame::DataRead r(cache_info_table.frames[curr_frame], desc_info.sem, desc_info.data_lock);

				// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset)
				//  gives us the address of the first byte to copy which may or may not be on a page boundary.
				//
				// We can copy only CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset) which gives us the number
				//  of bytes from the current offset to the end of the page.
				memcpy(
						(uint8_t *)buffer + buffer_offset,
						r.ptr() + CS_PARTIAL_SIZE(initial_start_offset),
						initial_end_offset - initial_start_offset);
			}

			buffer_offset += initial_end_offset - initial_start_offset;
		}

		// Pages in the middle must be copied in full.
		while (buffer_offset < CS_GET_PAGE(length)) {

			{
				if (unlikely((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == (size_t)~0)) {
					_err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Condition ' " _STR((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == (size_t)~0) " ' is true. returned: " _STR((size_t)~0));
					return (size_t)~0;
				} else
					_err_error_exists = false;
			}
			// Get page holder mapped to page.

			//ERR_FAIL_COND_V((curr_frame = cache_info_table.page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			{
				if (unlikely((curr_frame = cache_info_table.page_frame_map[curr_page]) == (size_t)~0)) {
					_err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Condition ' " _STR((curr_frame = cache_info_table.page_frame_map[curr_page]) == (size_t)~0) " ' is true. returned: " _STR((size_t)~0));
					return (size_t)~0;
				} else
					_err_error_exists = false;
			}

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset) gives us the start
			WARN_PRINT("Reading intermediate page.");

			// Lock current page holder.
			{
				Frame::DataRead r(cache_info_table.frames[curr_frame], desc_info.sem, desc_info.data_lock);

				memcpy(
						(uint8_t *)buffer + buffer_offset,
						r.ptr(),
						CS_PAGE_SIZE);
			}

			buffer_offset += CS_PAGE_SIZE;
		}

		// For final potentially pageially filled page
		if (initial_end_offset == CS_PAGE_SIZE && final_partial_length) {

			{
				if (unlikely((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == (size_t)~0)) {
					_err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Condition ' " _STR((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) == (size_t)~0) " ' is true. returned: " _STR((size_t)~0));
					return (size_t)~0;
				} else
					_err_error_exists = false;
			}
			// Get page holder mapped to page.

			//ERR_FAIL_COND_V((curr_frame = cache_info_table.page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			{
				if (unlikely((curr_frame = cache_info_table.page_frame_map[curr_page]) == (size_t)~0)) {
					_err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Condition ' " _STR((curr_frame = cache_info_table.page_frame_map[curr_page]) == (size_t)~0) " ' is true. returned: " _STR((size_t)~0));
					return (size_t)~0;
				} else
					_err_error_exists = false;
			}

			WARN_PRINT("Reading last page.");

			{ // Lock last page for reading data.
				Frame::DataRead r(cache_info_table.frames[curr_frame], desc_info.sem, desc_info.data_lock);

				memcpy(
						(uint8_t *)buffer + buffer_offset,
						r.ptr(),
						final_partial_length);
			}
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
		DescriptorInfo &desc_info = **elem;
		size_t initial_start_offset = desc_info.offset;
		size_t initial_end_offset = initial_start_offset + CS_PAGE_SIZE;
		size_t final_partial_length = CS_PARTIAL_SIZE(initial_start_offset + length);
		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;
		size_t data_offset = 0;

		// We need to handle the first and last frames differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{
			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			ERR_FAIL_COND_V((curr_frame = cache_info_table.page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// The end offset of the first page may not be greater than the start offset of the next page.
			initial_end_offset = CLAMP(initial_start_offset + length, 0, CS_GET_PAGE(initial_start_offset) + CS_PAGE_SIZE);

			WARN_PRINT("Reading first page.");

			{ // Lock the page holder for the operation.
				Frame::DataWrite w(cache_info_table.frames[curr_frame], desc_info.data_lock);

				// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset)
				//  gives us the address of the first byte to copy which may or may not be on a page boundary.
				//
				// We can copy only CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset) which gives us the number
				//  of bytes from the current offset to the end of the page.
				memcpy(
						w.ptr() + CS_PARTIAL_SIZE(initial_start_offset),
						(uint8_t *)data + data_offset,
						initial_end_offset - initial_start_offset);
			}

			data_offset += initial_end_offset - initial_start_offset;
		}

		// Pages in the middle must be copied in full.
		while (data_offset < CS_GET_PAGE(length)) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			ERR_FAIL_COND_V((curr_frame = cache_info_table.page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset) gives us the start
			WARN_PRINT("Reading intermediate page.");

			// Lock current page holder.
			{
				Frame::DataWrite w(cache_info_table.frames[curr_frame], desc_info.data_lock);

				memcpy(
						w.ptr(),
						(uint8_t *)data + data_offset,
						CS_PAGE_SIZE);
			}

			data_offset += CS_PAGE_SIZE;
		}

		// For final potentially pageially filled page
		if (initial_end_offset == CS_PAGE_SIZE && final_partial_length) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			ERR_FAIL_COND_V((curr_frame = cache_info_table.page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			WARN_PRINT("Reading last page.");

			{ // Lock last page for reading data.
				Frame::DataWrite w(cache_info_table.frames[curr_frame], desc_info.data_lock);

				memcpy(
						w.ptr(),
						(uint8_t *)data + data_offset,
						final_partial_length);
			}
			data_offset += final_partial_length;
		}

		desc_info.offset += data_offset;

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

		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;

		if (eff_offset < 0) {
			ERR_PRINT("Invalid offset.")
			return CS_MEM_VAL_BAD;

			// In this case, there can only be a hole and no data, so we only
			// do a paging operation to represent it in memory, in case of a write.
		} else if (eff_offset > end_offset) {
			WARN_PRINT(("Seek op with effective offset" + itos(eff_offset)).utf8().get_data());
			for (int i = 0; i < CS_SEEK_READ_AHEAD_SIZE; i++) {
				check_with_page_op(desc_info, eff_offset + i * CS_PAGE_SIZE);
				WARN_PRINT(("Checked page " + itos(CS_GET_PAGE(eff_offset + i * CS_PAGE_SIZE))).utf8().get_data());
			}

		} else {
			for (int i = 0; i < CS_SEEK_READ_AHEAD_SIZE; i++) {
				check_with_page_op_and_update(desc_info, &curr_page, &curr_frame, eff_offset + i * CS_PAGE_SIZE);
				enqueue_load(desc_info, curr_page);
				WARN_PRINT(("Checked and enqueued load of page " + itos(CS_GET_PAGE(eff_offset + i * CS_PAGE_SIZE))).utf8().get_data());
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
	page_id curr_page = CS_GET_PAGE(offset);
	if (get_page_guid(*desc_info, offset, true) == CS_MEM_VAL_BAD) {

		page_id cp = get_page_guid(*desc_info, offset, false);
		frame_id cf = CS_MEM_VAL_BAD;

		cache_info_table.pages.ordered_insert(cp);

		do_paging_op(desc_info, cp, &cf);
		ERR_FAIL_COND_V(cf == CS_MEM_VAL_BAD, false);

		cache_info_table.page_frame_map.insert(cp, cf);

		return false;
	}
	enforce_cache_policy(desc_info, curr_page);
	return true;
}

bool FileCacheManager::check_with_page_op_and_update(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame, size_t offset) {

	if (get_page_guid(*desc_info, offset, true) == CS_MEM_VAL_BAD) {

		page_id cp = get_page_guid(*desc_info, offset, false);
		frame_id cf = CS_MEM_VAL_BAD;

		cache_info_table.pages.ordered_insert(cp);

		do_paging_op(desc_info, cp, &cf);
		ERR_FAIL_COND_V(cf == CS_MEM_VAL_BAD, false);

		cache_info_table.page_frame_map.insert(cp, cf);

		if (curr_frame)
			*curr_frame = cf;
		if (curr_page)
			*curr_page = cp;

		return false;
	}

	return true;
}

FileCacheManager *FileCacheManager::singleton = NULL;
_FileCacheManager *_FileCacheManager::singleton = NULL;

FileCacheManager *FileCacheManager::get_singleton() {
	return singleton;
}

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

void FileCacheManager::thread_func(void *p_udata) {
	srandom(time(0));
	FileCacheManager &fcs = *static_cast<FileCacheManager *>(p_udata);

	do {

		//		WARN_PRINT(("Thread" + itos(fcs.thread->get_id())  +   "Waiting for message.").utf8().get_data());
		CtrlOp l = fcs.op_queue.pop();
		WARN_PRINT("got message");
		if (l.type == CtrlOp::QUIT) break;
		ERR_CONTINUE(l.di == NULL);

		page_id curr_page = CS_GET_PAGE(l.offset);
		frame_id curr_frame = fcs.cache_info_table.page_frame_map[curr_page];

		switch (l.type) {
			case CtrlOp::LOAD: {
				WARN_PRINT(("Performing load for offset " + itos(l.offset) + "\nIn pages: " + itos(CS_GET_PAGE(l.offset))).utf8().get_data());
				fcs.do_load_op(l.di, curr_page, curr_frame, l.offset);
				break;
			}
			case CtrlOp::STORE: {
				WARN_PRINT("Performing store.");
				fcs.do_store_op(l.di, curr_page, curr_frame, l.offset);
				break;
			}
			default: ERR_FAIL();
		}
	} while (!fcs.exit_thread);
}

void FileCacheManager::check_cache(const RID *const rid, size_t length) {
	DescriptorInfo *desc_info = files[rid->get_id()];

	for (int i = desc_info->offset; i < desc_info->offset + length; i += CS_PAGE_SIZE) {
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
