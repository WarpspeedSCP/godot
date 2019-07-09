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
//static uint8_t zero[CS_PAGE_SIZE] = {0};
#define RID_TO_DD(op) (size_t) rid op get_id() & 0x0000000000FFFFFF
#define RID_PTR_TO_DD RID_TO_DD(->)
#define RID_REF_TO_DD RID_TO_DD(.)

FileCacheManager::FileCacheManager() {
	mutex = Mutex::create();
	rng.set_seed(OS::get_singleton()->get_ticks_usec());

	memset(zero, 0, CS_PAGE_SIZE);
	memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);
	page_frame_map.clear();
	// pages.clear();
	frames.clear();

	available_space = CS_CACHE_SIZE;
	used_space = 0;
	total_space = CS_CACHE_SIZE;

	for (size_t i = 0; i < CS_NUM_FRAMES; ++i) {
		frames.push_back(
				memnew(Frame(memory_region + i * CS_PAGE_SIZE)));
	}

	singleton = this;
}

FileCacheManager::~FileCacheManager() {
	WARN_PRINT("Destructor running.");
	if (memory_region) memdelete(memory_region);

	if (files.size()) {
		const data_descriptor *key = NULL;
		for (key = files.next(NULL); key; key = files.next(key)) {
			memdelete(files[*key]);
		}
	}

	for (int i = 0; i < frames.size(); ++i) {
		memdelete(frames[i]);
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

data_descriptor FileCacheManager::add_data_source(RID *rid, FileAccess *data_source, int cache_policy) {
	ERR_FAIL_COND_V(!rid->is_valid(), CS_MEM_VAL_BAD);
	data_descriptor dd = RID_PTR_TO_DD;

	files[dd] = memnew(DescriptorInfo(data_source, (size_t)dd << 40, cache_policy));
	WARN_PRINTS(files[dd]->abs_path);
	const data_descriptor *key = files.next(NULL);
	for (; key; key = files.next(key)) {
		printf("\t\t%lx\n", files[*key]);
	}
	ERR_FAIL_COND_V(files[dd] == NULL, CS_MEM_VAL_BAD);
	seek(rid, 0, SEEK_SET);
	check_cache(rid, (cache_policy == _FileCacheManager::KEEP ? CS_KEEP_THRESH_DEFAULT : cache_policy == _FileCacheManager::LRU ? CS_LRU_THRESH_DEFAULT : CS_FIFO_THRESH_DEFAULT) * CS_PAGE_SIZE);
	return dd;
}

void FileCacheManager::remove_data_source(data_descriptor dd) {
	if (files.has(dd)) {
		DescriptorInfo *di = files[dd];
		for (int i = 0; i < di->pages.size(); i++) {
			Frame::MetaWrite(
					frames[page_frame_map[di->pages[i]]],
					di->meta_lock)
					.set_used(false)
					.set_ready_false();
			memset(
					Frame::DataWrite(
							frames[page_frame_map[di->pages[i]]],
							di->sem,
							di->data_lock)
							.ptr(),
					0,
					4096);
		}

		memdelete(files[dd]);
		files.erase(dd);
	}
}

// !!! takes mutable references to all params.
// Takes an extra_offset parameter that we use to keep track
// of temporary updates to the current offset in the file.
void FileCacheManager::do_load_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	// Get data from data source somehow...

	if ((offset + CS_PAGE_SIZE) < desc_info->total_size) {
		if (check_incomplete_page_load(desc_info, curr_page, curr_frame, offset))
			// { _err_print_error(FUNCTION_STR, __FILE__, __LINE__, "Read less than " "0x1000" " bytes."); _err_error_exists = false; }
			ERR_PRINTS("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
		else {
			ERR_PRINTS("Read size : " + itoh(frames[curr_frame]->used_size));
		}
	} else {
		check_incomplete_page_load(desc_info, curr_page, curr_frame, offset);
		ERR_PRINTS("Read " + itoh(frames[curr_frame]->used_size) + " bytes at end of file.");
	}
}

// !!! takes mutable references to all params.
void FileCacheManager::do_store_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	// store back to data source somehow...

	if (check_incomplete_page_store(desc_info, curr_page, curr_frame, offset)) {
		ERR_PRINTS("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
	} else {
		ERR_PRINT("Read a page");
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
		Frame::DataWrite w(frames[curr_frame], desc_info->sem, desc_info->data_lock);
		used_size = desc_info->internal_data_source->get_buffer(w.ptr(), CS_PAGE_SIZE);
		s = String((char *)w.ptr()) + " ";
		Frame::MetaWrite(frames[curr_frame], desc_info->meta_lock).set_used_size(used_size).set_ready_true(desc_info->sem);
	}
	ERR_PRINTS("Loaded " + itoh(used_size) + " from offset " + itoh(offset) + " with page " + itoh(curr_page) + " mapped to frame " + itoh(curr_frame))
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
		Frame::DataRead r(frames[curr_frame], desc_info->sem, desc_info->data_lock);
		desc_info->internal_data_source->store_buffer(r.ptr(), CS_PAGE_SIZE);
		Frame::MetaWrite(frames[curr_frame], desc_info->meta_lock).set_dirty(false);
	}
	return desc_info->internal_data_source->get_error() == ERR_FILE_CANT_WRITE;
}

// Perform a read operation.
size_t FileCacheManager::read(const RID *const rid, void *const buffer, size_t length) {

	DescriptorInfo **elem = files.getptr(rid->get_id());

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo *desc_info = *elem;
		size_t read_length = length;

		if ((desc_info->offset + read_length) / desc_info->total_size > 0) {
			WARN_PRINTS("Reached EOF before reading " + itoh(read_length) + " bytes.");
			read_length = desc_info->total_size - desc_info->offset;
		}

		size_t initial_start_offset = desc_info->offset;
		size_t initial_end_offset = initial_start_offset + CS_PAGE_SIZE;
		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;
		size_t buffer_offset = 0;

		// We need to handle the first and last frames differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{

			WARN_PRINTS("Getting page for offset " + itoh(desc_info->offset + buffer_offset) + " with start offset " + itoh(desc_info->offset))
			// Query for the page with the current offset.
			CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + buffer_offset, true)) == CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);

			// The end offset of the first page may not be greater than the start offset of the next page.
			initial_end_offset = MIN(initial_start_offset + length, initial_end_offset);
			WARN_PRINTS("Reading first page with values:\ninitial_start_offset: " + itoh(initial_start_offset) + "\ninitial_end_offset: " + itoh(initial_end_offset) + "\n read size: " + itoh(initial_end_offset - initial_start_offset));

			{ // Lock the page holder for the operation.
				Frame::DataRead r(frames[curr_frame], desc_info->sem, desc_info->data_lock);

				// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info->offset)
				//  gives us the address of the first byte to copy which may or may not be on a page boundary.
				//
				// We can copy only CS_PAGE_SIZE - PARTIAL_SIZE(desc_info->offset) which gives us the number
				//  of bytes from the current offset to the end of the page.
				memcpy(
						(uint8_t *)buffer + buffer_offset,
						r.ptr() + CS_PARTIAL_SIZE(initial_start_offset), //CLAMP(, CS_GET_PAGE(initial_start_offset), Frame::MetaRead(frames[curr_frame], desc_info->meta_lock).get_last_use()),
						initial_end_offset - initial_start_offset);
			}

			buffer_offset += (initial_end_offset - initial_start_offset);
			read_length -= (initial_end_offset - initial_start_offset);
		}

		// Pages in the middle must be copied in full.
		while (buffer_offset < CS_GET_PAGE(length) && read_length > CS_PAGE_SIZE) {

			// Query for the page with the current offset.
			CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + buffer_offset, true)) == CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);
			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info->offset) gives us the start

			WARN_PRINTS("Reading intermediate page.\nbuffer_offset: " + itoh(buffer_offset) + "\nread_length: " + itoh(read_length) + "\ncurrent offset: " + itoh(desc_info->offset));

			// Lock current frame.
			{
				Frame::DataRead r(frames[curr_frame], desc_info->sem, desc_info->data_lock);

				memcpy(
						(uint8_t *)buffer + buffer_offset,
						r.ptr(),
						CS_PAGE_SIZE);
			}

			buffer_offset += CS_PAGE_SIZE;
			read_length -= CS_PAGE_SIZE;
		}

		// For final potentially partially filled page
		if (read_length) {

			// Query for the page with the current offset.
			CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + buffer_offset, true)) == CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);

			// CRASH_COND(final_partial_length > Frame::MetaRead(frames[curr_frame], desc_info->meta_lock).get_used_size());

			size_t temp_read_len = CLAMP(read_length, 0, Frame::MetaRead(frames[curr_frame], desc_info->meta_lock).get_used_size());
			WARN_PRINTS("Reading last page.\nread_length: " + itoh(read_length) + "\ntemp_read_len: " + itoh(temp_read_len));

			{ // Lock last page for reading data.
				Frame::DataRead r(frames[curr_frame], desc_info->sem, desc_info->data_lock);

				memcpy(
						(uint8_t *)buffer + buffer_offset,
						r.ptr(),
						temp_read_len);
			}
			buffer_offset += temp_read_len;
			read_length -= temp_read_len;
		}

		if (read_length > 0) WARN_PRINTS("Read less than " + itoh(length - read_length) + " bytes.")

		// TODO: Document this. Reads that exceed EOF will cause the remaining buffer space to be zeroed out.
		if ((desc_info->offset + length) / desc_info->total_size > 0) {
			memset(buffer + (desc_info->total_size - desc_info->offset), 'B', length - read_length);
		}

		// We update the current offset at the end of the operation.
		desc_info->offset += buffer_offset;
		return buffer_offset;
	}
}

// Similar to the read operation but opposite data flow.
size_t FileCacheManager::write(const RID *const rid, const void *const data, size_t length) {
	DescriptorInfo **elem = files.getptr(rid->get_id());
	// TODO: Copy on write functionality for OOB writes.
	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {
		DescriptorInfo *desc_info = *elem;
		size_t initial_start_offset = desc_info->offset;
		size_t initial_end_offset = initial_start_offset + CS_PAGE_SIZE;
		size_t final_partial_length = CS_PARTIAL_SIZE(initial_start_offset + length);
		page_id curr_page = CS_MEM_VAL_BAD;
		frame_id curr_frame = CS_MEM_VAL_BAD;
		size_t data_offset = 0;

		// We need to handle the first and last frames differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{
			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info->offset + data_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// The end offset of the first page may not be greater than the start offset of the next page.
			initial_end_offset = CLAMP(initial_start_offset + length, 0, CS_GET_PAGE(initial_start_offset) + CS_PAGE_SIZE);

			WARN_PRINT("Reading first page.");

			{ // Lock the page holder for the operation.
				Frame::DataWrite w(frames[curr_frame], desc_info->sem, desc_info->data_lock);

				// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info->offset)
				//  gives us the address of the first byte to copy which may or may not be on a page boundary.
				//
				// We can copy only CS_PAGE_SIZE - PARTIAL_SIZE(desc_info->offset) which gives us the number
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
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info->offset + data_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info->offset) gives us the start
			WARN_PRINT("Reading intermediate page.");

			// Lock current page holder.
			{
				Frame::DataWrite w(frames[curr_frame], desc_info->sem, desc_info->data_lock);

				memcpy(
						w.ptr(),
						(uint8_t *)data + data_offset,
						CS_PAGE_SIZE);
			}

			data_offset += CS_PAGE_SIZE;
		}

		// For final potentially partially filled page
		if (initial_end_offset == CS_PAGE_SIZE && final_partial_length) {

			// Query for the page with the current offset.
			ERR_FAIL_COND_V((curr_page = get_page_guid(desc_info, desc_info->offset + data_offset, true)) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
			// Get page holder mapped to page.
			ERR_FAIL_COND_V((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);

			CRASH_COND(curr_page + final_partial_length > curr_page + CS_PAGE_SIZE);

			WARN_PRINT("Reading last page.");

			{ // Lock last page for reading data.
				Frame::DataWrite w(frames[curr_frame], desc_info->sem, desc_info->data_lock);

				memcpy(
						w.ptr(),
						(uint8_t *)data + data_offset,
						final_partial_length);
			}
			data_offset += final_partial_length;
		}

		desc_info->offset += data_offset;

		return data_offset;
	}
}

// The seek operation just uses the POSIX seek modes, which will probably get replaced later.
size_t FileCacheManager::seek(const RID *const rid, size_t new_offset, int mode) {
	DescriptorInfo *elem = files[(data_descriptor)RID_PTR_TO_DD];

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo *desc_info = elem;
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
		}

		if (eff_offset - curr_offset > CS_FIFO_THRESH_DEFAULT) {
			/** when the user seeks far away from the current offset,
			 * if the io queue currently has pending operations, maybe
			 * we can clear the queue. That way, we can read ahead at the
			 * new offset without any waits, and we only need to do an inexpensive page_id replacement operation instead of waiting for a load to occur.
			*/
			CtrlOp l;
			// This executes on the main thread.
			// Lock the operation queue to prevent the remaining pages from loading.
			if (!op_queue.queue.empty()) {
				op_queue.lock();
				while (!op_queue.queue.empty()) {
					WARN_PRINT("Acquired client side queue lock.");
					// If the next operation is  a load op, we can remove it from the op queue.
					if (op_queue.queue.front()->get().type == CtrlOp::LOAD)
						l = op_queue.pop();
					else {
						// If the next operation is a store op, we must allow the IO thread to perform the operation.
						op_queue.unlock();
						// Hopefully this will allow the IO thread to acquire the lock before the statement below is executed.
						op_queue.lock();
						// At this point, the IO thread should have popped the store op off the queue.
						// We can now continue to the next iteration.
						continue;
					}
					WARN_PRINTS("Unmapping page " + itoh(CS_GET_PAGE(l.offset)) + " and frame " + itoh(l.frame));
					l.di->pages.erase(CS_GET_PAGE(l.offset));
					page_frame_map.erase(CS_GET_PAGE(l.offset));
					CS_GET_CACHE_POLICY_FN(cache_removal_policies, desc_info->cache_policy)
					(desc_info->guid_prefix | CS_GET_PAGE(l.offset));
					Frame::MetaWrite(frames[l.frame], l.di->meta_lock).set_used(false).set_ready_false();
				}
				WARN_PRINT("Released client side queue lock.");
				op_queue.unlock();
			}
		}

		// Update the offset.

		desc_info->offset = eff_offset;

		// Update descriptor info.
		// file_page_map.insert(dd, desc_info);

		return eff_offset;
	}
}

void FileCacheManager::flush(const RID *const rid) {
	DescriptorInfo *desc_info = files[RID_PTR_TO_DD];
	for (int i = 0; i < desc_info->pages.size(); i++) {

		enqueue_store(desc_info, page_frame_map[desc_info->pages[i]], CS_GET_PAGE(desc_info->pages[i]));
	}
}

size_t FileCacheManager::get_len(const RID *const rid) const {
	data_descriptor dd = RID_PTR_TO_DD;
	size_t size = files[dd]->internal_data_source->get_len();
	if (size > files[dd]->total_size) {
		files[dd]->total_size = size;
	}

	return size;
}

RID FileCacheManager::open(const String &path, int p_mode, int cache_policy) {
	printf("%s %d\n", path.utf8().get_data(), p_mode);

	ERR_FAIL_COND_V(!mutex, RID());
	ERR_FAIL_COND_V(path.empty(), RID());

	MutexLock ml = MutexLock(mutex);

	// Will be freed when close is called with the corresponding RID.
	CachedResourceHandle *hdl = memnew(CachedResourceHandle);
	RID rid = handle_owner.make_rid(hdl);

	ERR_FAIL_COND_V(!rid.is_valid(), RID());
	FileAccess *fa = FileAccess::open(path, p_mode);
	add_data_source(&rid, fa, cache_policy);
	printf("open file %s with mode %d\nGot RID %d\n", path.utf8().get_data(), p_mode, RID_REF_TO_DD);
	return rid;
}

bool FileCacheManager::file_exists(const String &p_name) const {
	FileAccess *f = FileAccess::create(FileAccess::ACCESS_FILESYSTEM);
	bool exists = f->file_exists(p_name);
	memdelete(f);
	return exists;
}

bool FileCacheManager::eof_reached(const RID *const rid) const {
	bool eof = files[RID_PTR_TO_DD]->internal_data_source->eof_reached();
	return eof;
}

void FileCacheManager::rp_lru(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame) {
	page_id page_to_evict = CS_MEM_VAL_BAD;
	frame_id frame_to_evict = CS_MEM_VAL_BAD;

	if (lru_cached_pages.size() > CS_LRU_THRESH_DEFAULT) {
		Frame *f = frames.operator[](page_frame_map.operator[](lru_cached_pages.back()->get()));
		DescriptorInfo *d = files.operator[](lru_cached_pages.back()->get() >> 40);

		if (step - Frame::MetaRead(f, d->meta_lock).get_last_use() > CS_LRU_THRESH_DEFAULT) {

			page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : lru_cached_pages.back()->prev()->get();

			lru_cached_pages.erase(page_to_evict);

			frame_to_evict = page_frame_map[page_to_evict];

			WARN_PRINTS("evicted page " + itoh(page_to_evict));
			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_dirty(false).set_used(true).set_last_use(step).set_ready_false();
			}

			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;

			ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);

		} else if (fifo_cached_pages.size() > CS_FIFO_THRESH_DEFAULT / 2) {

			page_to_evict = fifo_cached_pages.back()->get();

			fifo_cached_pages.erase(fifo_cached_pages.back());

			frame_to_evict = page_frame_map[page_to_evict];

			WARN_PRINTS("evicted page " + itoh(page_to_evict));
			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_dirty(false).set_used(true).set_last_use(step).set_ready_false();
			}
			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;

			ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);

		} else if (lru_cached_pages.size()) {

			page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : lru_cached_pages.back()->prev()->get();

			lru_cached_pages.erase(page_to_evict);

			frame_to_evict = page_frame_map[page_to_evict];

			WARN_PRINTS("evicted page " + itoh(page_to_evict));
			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_dirty(false).set_used(true).set_last_use(step).set_ready_false();
			}

			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;

			ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);

		} else
			CRASH_NOW();
	}

	WARN_PRINTS("evicted page under LRU " + itoh(page_to_evict));
	// get the guid prefix and shift it to lsb to get the RID key.
	files[(page_to_evict >> 40)]->pages.erase(page_to_evict);
	page_frame_map.erase(page_to_evict);
	page_frame_map.insert(*curr_page, *curr_frame);
	CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
	(*curr_page);
}

void FileCacheManager::rp_keep(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame) {
	page_id page_to_evict = CS_MEM_VAL_BAD;
	frame_id frame_to_evict = CS_MEM_VAL_BAD;

	if (fifo_cached_pages.size() > CS_FIFO_THRESH_DEFAULT / 2) {

		page_to_evict = fifo_cached_pages.back()->get();

		fifo_cached_pages.erase(fifo_cached_pages.back());

		frame_to_evict = page_frame_map[page_to_evict];

		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_last_use(step).set_ready_false();
		}

		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

		ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);

	} else {

		if (lru_cached_pages.size() > CS_LRU_THRESH_DEFAULT / 2) {

			Frame *f = frames.operator[](page_frame_map.operator[](lru_cached_pages.back()->get()));
			DescriptorInfo *d = files.operator[](lru_cached_pages.back()->get() >> 40);

			if (step - Frame::MetaRead(f, d->meta_lock).get_last_use() > CS_LRU_THRESH_DEFAULT) {

				page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : permanent_cached_pages.back()->prev()->get();

				lru_cached_pages.erase(page_to_evict);

				frame_to_evict = page_frame_map[page_to_evict];

				{
					Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
					if (w.get_dirty()) {
						enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
					}
					w.set_dirty(false).set_used(true).set_last_use(step).set_ready_false();
				}

				// We reuse the page holder we evicted.
				*curr_frame = frame_to_evict;

				ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);
			}
		} else if (permanent_cached_pages.size() > CS_KEEP_THRESH_DEFAULT / 2) {

			page_to_evict = (rng.randi() % 2) ? permanent_cached_pages.back()->get() : permanent_cached_pages.back()->prev()->get();

			permanent_cached_pages.erase(page_to_evict);

			frame_to_evict = page_frame_map[page_to_evict];

			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_dirty(false).set_used(true).set_last_use(step).set_ready_false();
			}

			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;

			ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);
		}
	}

	WARN_PRINTS("evicted page under KEEP " + itoh(page_to_evict));
	// get the guid prefix and shift it to lsb to get the RID key.
	files[(page_to_evict >> 40)]->pages.erase(page_to_evict);
	page_frame_map.erase(page_to_evict);
	page_frame_map.insert(*curr_page, *curr_frame);
	CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
	(*curr_page);
}

void FileCacheManager::rp_fifo(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame) {

	page_id page_to_evict = CS_MEM_VAL_BAD;
	frame_id frame_to_evict = CS_MEM_VAL_BAD;

	if (fifo_cached_pages.size()) {

		page_to_evict = fifo_cached_pages.back()->get();

		fifo_cached_pages.back()->erase();

		frame_to_evict = page_frame_map[page_to_evict];

		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_dirty(false).set_used(true).set_ready_false();
		}
		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

		ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);

	} else {
		if (lru_cached_pages.size() > CS_LRU_THRESH_DEFAULT / 2) {

			Frame *f = frames.operator[](page_frame_map.operator[](lru_cached_pages.back()->get()));
			DescriptorInfo *d = files.operator[](lru_cached_pages.back()->get() >> 40);

			if (step - Frame::MetaRead(f, d->meta_lock).get_last_use() > CS_LRU_THRESH_DEFAULT) {

				page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : lru_cached_pages.back()->prev()->get();

				lru_cached_pages.erase(page_to_evict);

				frame_to_evict = page_frame_map[page_to_evict];

				{
					Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
					if (w.get_dirty()) {
						enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
					}
					w.set_dirty(false).set_used(true).set_last_use(step).set_ready_false();
				}

				// We reuse the page holder we evicted.
				*curr_frame = frame_to_evict;

				ERR_FAIL_COND(*curr_frame == CS_MEM_VAL_BAD);
			}
		}
	}

	WARN_PRINTS("evicted page under FIFO " + itoh(page_to_evict));
	// get the guid prefix and shift it to lsb to get the RID key.
	files[(page_to_evict >> 40)]->pages.erase(page_to_evict);
	page_frame_map.erase(page_to_evict);
	page_frame_map.insert(*curr_page, *curr_frame);
	CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
	(*curr_page);
}

bool FileCacheManager::get_or_do_page_op(DescriptorInfo *desc_info, size_t offset, page_id *curr_page, frame_id *curr_frame) {

	page_id cp = get_page_guid(desc_info, offset, true);
	WARN_PRINT(("cp query: " + itoh(cp)).utf8().get_data());
	frame_id cf = CS_MEM_VAL_BAD;
	bool ret;

	if (cp == CS_MEM_VAL_BAD) {

		cp = get_page_guid(desc_info, offset, false);
		WARN_PRINT(("cp get: " + itoh(cp)).utf8().get_data());

		// do_paging_op(desc_info, cp, &cf);
		// Find a free frame.
		for (int i = 0; i < CS_NUM_FRAMES; ++i) {

			// TODO: change this to something more efficient.
			if (!frames[i]->used) {
				Frame::MetaWrite(
						frames[i], desc_info->meta_lock)
						.set_used(true)
						.set_last_use(step)
						.set_ready_false();
				cf = i;

				CRASH_COND(cf == CS_MEM_VAL_BAD);
				page_frame_map.insert(cp, cf);
				WARN_PRINT((itoh(cp) + " mapped to " + itoh(cf)).utf8().get_data());
				CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
				(cp);
				break;
			}
		}

		// If there are no free frames, we evict an old one according to the paging/caching algo (TODO).
		if (cf == (data_descriptor)CS_MEM_VAL_BAD) {
			WARN_PRINT("must evict");
			// Evict other page somehow...
			// Remove prev page-page holder mappings and associated pages.

			WARN_PRINTS("Cache policy: " + String(Dictionary(desc_info->to_variant(*this)).get("cache_policy", "-1")));

			CS_GET_CACHE_POLICY_FN(cache_replacement_policies, desc_info->cache_policy)
			(desc_info, &cp, &cf);

			WARN_PRINTS("curr_page : " + itoh(cp) + " mapped to curr_frame: " + itoh(cf));
		}

		desc_info->pages.ordered_insert(cp);

		if (curr_frame)
			*curr_frame = cf;
		if (curr_page)
			*curr_page = cp;

		ret = false;

	} else {
		// Update cache related details...
		CS_GET_CACHE_POLICY_FN(cache_update_policies, desc_info->cache_policy)
		(cp);
		ret = true;
	}

	{
		MutexLock ml(mutex);
		step += 1;
	}

	return ret;
}

void FileCacheManager::close(const RID *const rid) {
	MutexLock ml(mutex);
	files[RID_PTR_TO_DD]->valid = false;
	files[RID_PTR_TO_DD]->internal_data_source->close();
}

void FileCacheManager::permanent_close(const RID *const rid) {
	printf("permanently close file with RID %d\n", RID_PTR_TO_DD);
	MutexLock ml = MutexLock(mutex);
	data_descriptor dd = RID_PTR_TO_DD;
	handle_owner.free(*rid);
	memdelete(rid->get_data());
	remove_data_source(dd);
}

Error FileCacheManager::reopen(const RID *const rid, int mode) {
	DescriptorInfo *di = files[RID_PTR_TO_DD];
	if (!di->valid) {
		di->valid = true;
		return di->internal_data_source->reopen(di->abs_path, mode);

	} else {
		return di->internal_data_source->reopen(di->abs_path, mode);
		// FIXME: BAD SIDE EFFECTS OF CHANGING FILE MODES!!!
	}
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

		//ERR_PRINT(("Thread" + itoh(fcs.thread->get_id())  +   "Waiting for message.").utf8().get_data());
		CtrlOp l = fcs.op_queue.pop();
		//ERR_PRINT("got message");
		if (l.type == CtrlOp::QUIT) break;
		ERR_CONTINUE(l.di == NULL);

		page_id curr_page = get_page_guid(l.di, l.offset, false);
		frame_id curr_frame = fcs.page_frame_map[curr_page];

		switch (l.type) {
			case CtrlOp::LOAD: {
				//ERR_PRINT(("Performing load for offset " + itoh(l.offset) + "\nIn pages: " + itoh(CS_GET_PAGE(l.offset)) + "\nCurr page: " + itoh(curr_page) + "\nCurr frame: " + itoh(curr_frame)).utf8().get_data());
				fcs.do_load_op(l.di, curr_page, curr_frame, l.offset);
				break;
			}
			case CtrlOp::STORE: {
				ERR_PRINT("Performing store.");
				fcs.do_store_op(l.di, curr_page, curr_frame, l.offset);
				break;
			}
			default: ERR_FAIL();
		}
	} while (!fcs.exit_thread);
}

void FileCacheManager::check_cache(const RID *const rid, size_t length) {
	DescriptorInfo *desc_info = files[RID_PTR_TO_DD];
	if (length == CS_LEN_UNSPECIFIED) length = 8 * CS_PAGE_SIZE;

	for (int i = desc_info->offset; i < desc_info->offset + length; i += CS_PAGE_SIZE) {
		WARN_PRINTS("curr offset: " + itoh(i) + "");
		if (!get_or_do_page_op(desc_info, i, NULL, NULL)) {
			enqueue_load(desc_info, page_frame_map[desc_info->guid_prefix | CS_GET_PAGE(i)], i);
		}
	}
}

_FileCacheManager::_FileCacheManager() {
	singleton = this;
}

_FileCacheManager *_FileCacheManager::get_singleton() {
	return singleton;
}
