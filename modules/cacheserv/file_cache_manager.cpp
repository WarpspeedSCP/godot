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
// #include "file_access_unbuffered_unix.h"

#include "core/os/os.h"

#include <time.h>

static const bool CS_TRUE = true;
static const bool CS_FALSE = true;
// static uint8_t zero[CS_PAGE_SIZE] = { 0 };
#define RID_TO_DD(op) (size_t) rid op get_id() & 0x0000000000FFFFFF
#define RID_PTR_TO_DD RID_TO_DD(->)
#define RID_REF_TO_DD RID_TO_DD(.)

FileCacheManager::FileCacheManager() {
	mutex = Mutex::create();
	rng.set_seed(OS::get_singleton()->get_ticks_usec());

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

	op_queue.sig_quit = true;
	op_queue.push(CtrlOp());
	exit_thread = true;

	Thread::wait_to_finish(this->thread);

	memdelete(thread);
	memdelete(mutex);
}

RID FileCacheManager::open(const String &path, int p_mode, int cache_policy) {

	WARN_PRINTS(path + " " + itoh(p_mode));

	ERR_FAIL_COND_V(path.empty(), RID());

	MutexLock ml = MutexLock(mutex);

	RID rid;

	if (rids.has(path)) {
		WARN_PRINTS("file already exists, reopening.");

		rid = rids[path];
		DescriptorInfo *desc_info = files[rid.get_id() & 0x0000000000FFFFFF];

		ERR_COND_ACTION(
				desc_info->valid,
				String("This file is already open."),
				{ return RID(); });
		CRASH_COND(desc_info->internal_data_source != NULL);

		desc_info->internal_data_source = FileAccess::open(desc_info->path, p_mode);
		seek(rid, files[rid.get_id() & 0x0000000000FFFFFF]->offset);
		desc_info->valid = true;
		// Let the IO thread know that the file can be read from.
		desc_info->sem->post();

		if(desc_info->cache_policy != cache_policy) {
			for(int i = 0; i < desc_info->pages.size(); ++i) {
				CS_GET_CACHE_POLICY_FN(cache_removal_policies, desc_info->cache_policy)(desc_info->pages[i]);
				CS_GET_CACHE_POLICY_FN(cache_insertion_policies, cache_policy)(desc_info->pages[i]);
			}
		}

	} else {
		// Will be freed when close is called with the corresponding RID.
		CachedResourceHandle *hdl = memnew(CachedResourceHandle);
		rid = handle_owner.make_rid(hdl);

		ERR_COND_ACTION(!rid.is_valid(), String("Failed to create RID."), { memdelete(hdl); return RID(); });

		// PreferredFileAccessType *fa = memnew(FileAccessUnbufferedUnix);
		// //Fail with a bad RID if we can't open the file.
		// ERR_COND_ACTION(fa->unbuffered_open(path, p_mode) != OK, String("Failed to open file."), { memdelete(hdl); return RID(); });
		FileAccess *fa = NULL;
		CRASH_COND((fa = FileAccess::open(path, p_mode)) == NULL);

		rids[path] = (add_data_source(rid, fa, cache_policy));
		WARN_PRINTS("open file " + path + " with mode " + itoh(p_mode) + "\nGot RID " + itoh(RID_REF_TO_DD) + "\n");
	}

	return rid;
}

void FileCacheManager::close(const RID rid) {

	// TODO: Determine if this is necessary.
	// MutexLock ml(mutex);

	DescriptorInfo *const *elem = files.getptr(RID_REF_TO_DD);

	ERR_FAIL_COND_MSG(!elem, String("No such file"))

	DescriptorInfo *desc_info = *elem;

	// We'll delete the FileAccess pointer in the open function if we reopen this file.
	desc_info->valid = false;
	desc_info->internal_data_source->close();
	memdelete(desc_info->internal_data_source);
	desc_info->internal_data_source = NULL;
}

void FileCacheManager::permanent_close(const RID rid) {
	WARN_PRINTS("permanently closed file with RID " + itoh(RID_REF_TO_DD));
	MutexLock ml = MutexLock(mutex);
	remove_data_source(rid);
	handle_owner.free(rid);
	memdelete(rid.get_data());
}

// Error FileCacheManager::reopen(const RID rid, int mode) {
// 	DescriptorInfo *di = files[RID_REF_TO_DD];
// 	if (!di->valid) {
// 		di->valid = true;
// 		return di->internal_data_source->reopen(di->path, mode);

// 	} else {
// 		return di->internal_data_source->reopen(di->path, mode);
// 		// FIXME: BAD SIDE EFFECTS OF CHANGING FILE MODES!!!
// 	}
// }

/**
 * Register a file handle with the cache manager.
 * This function takes a pointer to a FileAccess object,
 * so anything that implements the FileAccess API (from the file system, or from the network)
 * can act as a data source.
 */
RID FileCacheManager::add_data_source(RID rid, FileAccess *data_source, int cache_policy) {

	CRASH_COND(rid.is_valid() == false);
	data_descriptor dd = RID_REF_TO_DD;

	files[dd] = memnew(DescriptorInfo(data_source, (size_t)dd << 40, cache_policy));
	files[dd]->valid = true;
	WARN_PRINTS(files[dd]->path);
	const data_descriptor *key = files.next(NULL);
	for (; key; key = files.next(key)) {
		printf("\t\t%lx\n", files[*key]);
	}
	CRASH_COND(files[dd] == NULL);
	seek(rid, 0, SEEK_SET);
	check_cache(rid, (cache_policy == _FileCacheManager::KEEP ? CS_KEEP_THRESH_DEFAULT : cache_policy == _FileCacheManager::LRU ? CS_LRU_THRESH_DEFAULT : CS_FIFO_THRESH_DEFAULT) * CS_PAGE_SIZE);

	return rid;
}

void FileCacheManager::remove_data_source(RID rid) {
	DescriptorInfo *di = files[rid.get_id() & 0x0000000000FFFFFF];
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
	rids.erase(di->path);
	files.erase(di->guid_prefix >> 40);
	memdelete(di->internal_data_source);
	memdelete(di);
}

// !!! takes mutable references to all params.
// Takes an extra_offset parameter that we use to keep track
// of temporary updates to the current offset in the file.
void FileCacheManager::do_load_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {
	// Get data from data source somehow...

	// Wait until the file is open.
	while(desc_info->valid != true) {
		desc_info->sem->wait();
		//ERR_EXPLAIN("File not open!")
		// CRASH_NOW()//(!desc_info->valid)
	}

	if ((offset + CS_PAGE_SIZE) < desc_info->total_size) {
		if (check_incomplete_page_load(desc_info, curr_page, curr_frame, offset)) {
			ERR_EXPLAIN("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.");
			CRASH_NOW();
		} else {
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

	// Wait until the file is open.
	while(!desc_info->valid) {
		desc_info->sem->wait();
		//ERR_EXPLAIN("File not open!")
		// CRASH_NOW()//(!desc_info->valid)
	}

	if (check_incomplete_page_store(desc_info, curr_page, curr_frame, offset)) {
		ERR_PRINTS("Wrote less than " STRINGIFY(CS_PAGE_SIZE) " bytes.");
	} else {
		ERR_PRINT("Read a page");
	}
}

// !!! takes mutable references to all params.
// The offset param is used to track temporary changes to file offset.
//
//  Returns true if a read from the current offset returns less than CS_PAGE_SIZE bytes.
//
// This operation updates the used_size value of the page holder.
_FORCE_INLINE_ bool FileCacheManager::check_incomplete_page_load(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset) {

	desc_info->internal_data_source->seek(CS_GET_FILE_OFFSET_FROM_GUID(curr_page));
	int64_t used_size;
	{
		Frame::DataWrite w(
				frames[curr_frame],
				desc_info->sem,
				desc_info->data_lock);

		used_size = desc_info->internal_data_source->get_buffer(
				w.ptr(),
				CS_PAGE_SIZE);
		ERR_EXPLAIN("File read returned " + itoh(used_size));

		// Error has occurred.
		CRASH_COND(used_size <= 0);
		Frame::MetaWrite(
				frames[curr_frame],
				desc_info->meta_lock)
				.set_used_size(used_size)
				.set_ready_true(desc_info->sem, curr_page, curr_frame);
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

	if (!desc_info->valid) {
		ERR_EXPLAIN("File not open!")
		CRASH_NOW() //(!desc_info->valid)
	}

	desc_info->internal_data_source->seek(CS_GET_PAGE(offset));
	{
		Frame::DataRead r(frames[curr_frame], desc_info->sem, desc_info->data_lock);
		desc_info->internal_data_source->store_buffer(r.ptr(), CS_PAGE_SIZE);
		Frame::MetaWrite(frames[curr_frame], desc_info->meta_lock).set_dirty_false(desc_info->sem);
	}
	return desc_info->internal_data_source->get_error() == ERR_FILE_CANT_WRITE;
}

// Perform a read operation.
size_t FileCacheManager::read(const RID rid, void *const buffer, size_t length) {

	DescriptorInfo **elem = files.getptr(RID_REF_TO_DD);

	ERR_FAIL_COND_MSG_V(!elem, String("No such file"), CS_MEM_VAL_BAD)

	DescriptorInfo *desc_info = *elem;
	size_t read_length = length;

	if ((desc_info->offset + read_length) / desc_info->total_size > 0) {
		WARN_PRINTS("Reached EOF before reading " + itoh(read_length) + " bytes.");
		read_length = desc_info->total_size - desc_info->offset;
	}

	size_t initial_start_offset = desc_info->offset;
	size_t initial_end_offset = CS_GET_PAGE(initial_start_offset + CS_PAGE_SIZE);
	page_id curr_page = CS_MEM_VAL_BAD;
	frame_id curr_frame = CS_MEM_VAL_BAD;
	size_t buffer_offset = 0;

	// We need to handle the first and last frames differently,
	// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
	{

		WARN_PRINTS("Getting page for offset " + itoh(desc_info->offset + buffer_offset) + " with start offset " + itoh(desc_info->offset))
		// Query for the page with the current offset.
		CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + buffer_offset, true)) == CS_MEM_VAL_BAD);
		// Get frame mapped to page.
		CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);

		// The end offset of the first page may not be greater than the start offset of the next page.
		initial_end_offset = MIN(initial_start_offset + read_length, initial_end_offset);
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
					r.ptr() + CS_PARTIAL_SIZE(initial_start_offset),
					initial_end_offset - initial_start_offset);
		}

		buffer_offset += (initial_end_offset - initial_start_offset);
		read_length -= buffer_offset;
	}

	// Pages in the middle must be copied in full.
	while (buffer_offset < CS_GET_PAGE(length) && read_length > CS_PAGE_SIZE) {

		// Query for the page with the current offset.
		CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + buffer_offset, true)) == CS_MEM_VAL_BAD);
		// Get frame mapped to page.
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
		// Get frame mapped to page.
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

	if (read_length > 0) WARN_PRINTS("Unread length: " + itoh(length - read_length) + " bytes.")

	CRASH_COND(read_length > 0);

	// TODO: Document this. Reads that exceed EOF will cause the remaining buffer space to be zeroed out.
	if ((desc_info->offset + length) / desc_info->total_size > 0) {
		memset((uint8_t *)buffer + (desc_info->total_size - desc_info->offset), '\0', length - read_length);
	}

	// We update the current offset at the end of the operation.
	desc_info->offset += buffer_offset;

	return buffer_offset;
}

// Similar to the read operation but opposite data flow.
size_t FileCacheManager::write(const RID rid, const void *const data, size_t length) {
	DescriptorInfo **elem = files.getptr(RID_REF_TO_DD);
	// TODO: Copy on write functionality for OOB writes.

	ERR_FAIL_COND_MSG_V(!elem, String("No such file"), CS_MEM_VAL_BAD)

	DescriptorInfo *desc_info = *elem;
	size_t write_length = length;

	size_t initial_start_offset = desc_info->offset;
	size_t initial_end_offset = CS_GET_PAGE(initial_start_offset + CS_PAGE_SIZE);
	page_id curr_page = CS_MEM_VAL_BAD;
	frame_id curr_frame = CS_MEM_VAL_BAD;
	size_t data_offset = 0;

	// We need to handle the first and last frames differently,
	// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
	{

		WARN_PRINTS("Getting page for offset " + itoh(desc_info->offset + data_offset) + " with start offset " + itoh(desc_info->offset))
		// Query for the page with the current offset.
		CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + data_offset, true)) == CS_MEM_VAL_BAD);
		// Get frame mapped to page.
		CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);

		// The end offset of the first page may not be greater than the start offset of the next page.
		initial_end_offset = MIN(initial_start_offset + write_length, initial_end_offset);
		WARN_PRINTS("Reading first page with values:\ninitial_start_offset: " + itoh(initial_start_offset) + "\ninitial_end_offset: " + itoh(initial_end_offset) + "\n read size: " + itoh(initial_end_offset - initial_start_offset));

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

		data_offset += (initial_end_offset - initial_start_offset);
		write_length -= data_offset;
	}

	// Pages in the middle must be copied in full.
	while (data_offset < CS_GET_PAGE(write_length) && write_length > CS_PAGE_SIZE) {

		// Query for the page with the current offset.
		CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + data_offset, true)) == CS_MEM_VAL_BAD);
		// Get frame mapped to page.
		CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);

		// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info->offset) gives us the start
		WARN_PRINTS("Writing intermediate page.\data_offset: " + itoh(data_offset) + "\nwrite_length: " + itoh(write_length) + "\ncurrent offset: " + itoh(desc_info->offset));

		// Lock current page holder.
		{
			Frame::DataWrite w(frames[curr_frame], desc_info->sem, desc_info->data_lock);

			memcpy(
					w.ptr(),
					(uint8_t *)data + data_offset,
					CS_PAGE_SIZE);
		}

		data_offset += CS_PAGE_SIZE;
		write_length -= CS_PAGE_SIZE;
	}

	// For final potentially partially filled page
	if (write_length) {

		// Query for the page with the current offset.
		CRASH_COND((curr_page = get_page_guid(desc_info, desc_info->offset + data_offset, true)) == CS_MEM_VAL_BAD);
		// Get frame mapped to page.
		CRASH_COND((curr_frame = page_frame_map[curr_page]) == CS_MEM_VAL_BAD);

		// CRASH_COND(curr_page + final_partial_length > curr_page + CS_PAGE_SIZE);

		size_t temp_write_len = CLAMP(write_length, 0, Frame::MetaRead(frames[curr_frame], desc_info->meta_lock).get_used_size());
		WARN_PRINTS("Writing last page.\nwrite_length: " + itoh(write_length) + "\ntemp_write_len: " + itoh(temp_write_len));

		{ // Lock last page for reading data.
			Frame::DataWrite w(frames[curr_frame], desc_info->sem, desc_info->data_lock);

			memcpy(
					w.ptr(),
					(uint8_t *)data + data_offset,
					temp_write_len);
		}
		data_offset += temp_write_len;
		write_length -= temp_write_len;
	}
	if (write_length > 0) WARN_PRINTS("Unwritten length: " + itoh(length - write_length) + " bytes.")

	CRASH_COND(write_length > 0);

	// If the write exceeds the file's
	desc_info->offset += data_offset;

	return data_offset;
}

// The seek operation just uses the POSIX seek modes, which will probably get replaced later.
size_t FileCacheManager::seek(const RID rid, int64_t new_offset, int mode) {

	DescriptorInfo **elem = files.getptr(RID_REF_TO_DD);

	ERR_FAIL_COND_MSG_V(!elem, String("No such file"), CS_MEM_VAL_BAD)

	DescriptorInfo *desc_info = *elem;
	size_t curr_offset = desc_info->offset;
	size_t end_offset = desc_info->total_size;
	int64_t eff_offset = 0;
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
	}

	/**
		 * when the user seeks far away from the current offset,
		 * if the io queue currently has pending operations,
		 * maybe we can clear the queue of operations that affect
		 * our current file (and only those operations). That way, we can read ahead at the
		 * new offset without any waits, and we only need to
		 * do an inexpensive page replacement operation instead
		 * of waiting for a load to occur.
		 */
	// This executes on the main thread.
	if (!op_queue.queue.empty()) {
		// Lock the operation queue to prevent the IO thread from consuming any more pages.
		op_queue.lock();
		WARN_PRINT("Acquired client side queue lock.");
		// Look for load ops with the same file that are farther than a threshold distance away from our effective offset and remove them.
		for (List<CtrlOp>::Element *i = op_queue.queue.front(); i; i = i->next()) {
			if (
					// If the type of operation is a load...
					i->get().type == CtrlOp::LOAD &&
					// And the operation is being performed on the same file...
					i->get().di->guid_prefix == desc_info->guid_prefix &&
					// And the distance between the pages
					// in the vicinity of the new region
					// and the current offset is large enough...
					ABSDIFF(
							eff_offset + (CS_FIFO_THRESH_DEFAULT * CS_PAGE_SIZE / 2),
							i->get().offset) > CS_FIFO_THRESH_DEFAULT) {

				CtrlOp l = i->get();
				i->erase();
				// We can unmap the pages.
				WARN_PRINTS(
						"Unmapping out of range page " +
						itoh(CS_GET_PAGE(l.offset)) +
						" and frame " + itoh(l.frame) +
						" for file with RID " +
						itoh(rid.get_id()));

				l.di->pages.erase(CS_GET_PAGE(l.offset));
				// Run the right cache removal policy function.
				CS_GET_CACHE_POLICY_FN(cache_removal_policies, desc_info->cache_policy)
				(desc_info->guid_prefix | CS_GET_PAGE(l.offset));
				page_frame_map.erase(CS_GET_PAGE(l.offset));
				Frame::MetaWrite(frames[l.frame], l.di->meta_lock).set_used(false).set_ready_false();
			}
		}
		WARN_PRINT("Released client side queue lock.");
		op_queue.unlock();
	}

	// Update the offset.
	desc_info->offset = eff_offset;

	return eff_offset;
}

void FileCacheManager::flush(const RID rid) {

	DescriptorInfo **elem = files.getptr(RID_REF_TO_DD);

	ERR_FAIL_COND_MSG(!elem, String("No such file"))

	DescriptorInfo *desc_info = *elem;

	for (int i = 0; i < desc_info->pages.size(); i++) {
		if (
				Frame::MetaRead(
						frames[page_frame_map[desc_info->pages[i]]],
						desc_info->meta_lock)
						.get_dirty()) {
			enqueue_store(
					desc_info,
					page_frame_map[desc_info->pages[i]],
					CS_GET_PAGE(desc_info->pages[i]));
		}
	}
}

size_t FileCacheManager::get_len(const RID rid) const {

	DescriptorInfo *const *elem = files.getptr(RID_REF_TO_DD);

	ERR_FAIL_COND_MSG_V(!elem, String("No such file"), -1);

	DescriptorInfo *desc_info = *elem;

	size_t size = desc_info->internal_data_source->get_len();
	if (size > desc_info->total_size) {
		desc_info->total_size = size;
	}

	return size;
}

bool FileCacheManager::file_exists(const String &p_name) const {
	FileAccess *f = FileAccess::create(FileAccess::ACCESS_FILESYSTEM);
	bool exists = f->file_exists(p_name);
	memdelete(f);
	return exists;
}

bool FileCacheManager::eof_reached(const RID rid) const {

	DescriptorInfo *const *elem = files.getptr(RID_REF_TO_DD);

	ERR_FAIL_COND_MSG_V(!elem, String("No such file"), true);

	return (*elem)->internal_data_source->eof_reached();
}

void FileCacheManager::rmp_lru(page_id curr_page) {
	WARN_PRINTS("Removing LRU page " + itoh(curr_page));
	if (lru_cached_pages.find(curr_page) != NULL)
		lru_cached_pages.erase(curr_page);
}

void FileCacheManager::rmp_fifo(page_id curr_page) {
	WARN_PRINTS("Removing FIFO page " + itoh(curr_page));
	if (fifo_cached_pages.find(curr_page) != NULL)
		fifo_cached_pages.erase(curr_page);
}

void FileCacheManager::rmp_keep(page_id curr_page) {
	WARN_PRINTS("Removing permanent page " + itoh(curr_page));
	if (permanent_cached_pages.find(curr_page) != NULL)
		permanent_cached_pages.erase(curr_page);
}

void FileCacheManager::ip_lru(page_id curr_page) {
	WARN_PRINT("LRU cached.");
	lru_cached_pages.insert(curr_page);
}

void FileCacheManager::ip_fifo(page_id curr_page) {
	WARN_PRINT("FIFO cached.");
	fifo_cached_pages.push_front(curr_page);
}

void FileCacheManager::ip_keep(page_id curr_page) {
	WARN_PRINT("Permanent cached.");
	permanent_cached_pages.insert(curr_page);
}

void FileCacheManager::up_lru(page_id curr_page) {
	WARN_PRINT(("Updating LRU page " + itoh(curr_page)).utf8().get_data());
	lru_cached_pages.erase(curr_page);
	Frame::MetaWrite(frames[page_frame_map[curr_page]], files[curr_page >> 40]->meta_lock).set_last_use(step);
	lru_cached_pages.insert(curr_page);
}
void FileCacheManager::up_fifo(page_id curr_page) {
	WARN_PRINT(("Updating FIFO page " + itoh(curr_page)).utf8().get_data());
}
void FileCacheManager::up_keep(page_id curr_page) {
	WARN_PRINT(("Updating Permanent page " + itoh(curr_page)).utf8().get_data());
	permanent_cached_pages.erase(curr_page);
	Frame::MetaWrite(frames[page_frame_map[curr_page]], files[curr_page >> 40]->meta_lock).set_last_use(step);
	permanent_cached_pages.insert(curr_page);
}

/**
 * LRU replacement policy.
 */
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

			CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

			WARN_PRINTS("evicted page " + itoh(page_to_evict));
			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_used(true).set_last_use(step).set_ready_false();
			}

			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;
		}
	} else if (fifo_cached_pages.size() > CS_FIFO_THRESH_DEFAULT) {

		page_to_evict = fifo_cached_pages.back()->get();

		fifo_cached_pages.erase(fifo_cached_pages.back());

		frame_to_evict = page_frame_map[page_to_evict];

		CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

		WARN_PRINTS("evicted page " + itoh(page_to_evict));
		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_last_use(step).set_ready_false();
		}
		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

	} else if (lru_cached_pages.size()) {

		page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : lru_cached_pages.back()->prev()->get();

		lru_cached_pages.erase(page_to_evict);

		frame_to_evict = page_frame_map[page_to_evict];

		CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

		WARN_PRINTS("evicted page " + itoh(page_to_evict));
		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_last_use(step).set_ready_false();
		}

		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

	} else {
		ERR_EXPLAIN("CANNOT ADD LRU PAGE TO CACHE; INSUFFICIENT SPACE.")
		CRASH_NOW()
	}

	WARN_PRINTS("evicted page under LRU " + itoh(page_to_evict));
	// get the guid prefix and shift it to lsb to get the RID key.
	files[(page_to_evict >> 40)]->pages.erase(page_to_evict);
	page_frame_map.erase(page_to_evict);
	CRASH_COND(page_frame_map.insert(*curr_page, *curr_frame) == NULL);
	CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
	(*curr_page);
}

void FileCacheManager::rp_keep(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame) {

	page_id page_to_evict = CS_MEM_VAL_BAD;
	frame_id frame_to_evict = CS_MEM_VAL_BAD;

	if (fifo_cached_pages.size() > CS_FIFO_THRESH_DEFAULT) {

		page_to_evict = fifo_cached_pages.back()->get();

		fifo_cached_pages.erase(fifo_cached_pages.back());

		frame_to_evict = page_frame_map[page_to_evict];

		CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_last_use(step).set_ready_false();
		}

		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

	} else if (lru_cached_pages.size() > CS_LRU_THRESH_DEFAULT) {

		Frame *f = frames.operator[](page_frame_map.operator[](lru_cached_pages.back()->get()));
		DescriptorInfo *d = files.operator[](lru_cached_pages.back()->get() >> 40);

		if (step - Frame::MetaRead(f, d->meta_lock).get_last_use() > CS_LRU_THRESH_DEFAULT) {

			page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : permanent_cached_pages.back()->prev()->get();

			lru_cached_pages.erase(page_to_evict);

			frame_to_evict = page_frame_map[page_to_evict];
			CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_used(true).set_last_use(step).set_ready_false();
			}

			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;
		}
	} else if (permanent_cached_pages.size() > CS_KEEP_THRESH_DEFAULT / 2) {

		page_to_evict = (rng.randi() % 2) ? permanent_cached_pages.back()->get() : permanent_cached_pages.back()->prev()->get();

		permanent_cached_pages.erase(page_to_evict);

		frame_to_evict = page_frame_map[page_to_evict];

		CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_last_use(step).set_ready_false();
		}

		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

	} else {
		ERR_EXPLAIN("CANNOT ADD PERMANENT PAGE TO CACHE; INSUFFICIENT SPACE.")
		CRASH_NOW()
	}

	WARN_PRINTS("evicted page under KEEP " + itoh(page_to_evict));
	// get the guid prefix and shift it to lsb to get the RID key.
	files[(page_to_evict >> 40)]->pages.erase(page_to_evict);
	page_frame_map.erase(page_to_evict);
	CRASH_COND(page_frame_map.insert(*curr_page, *curr_frame) == NULL);
	CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
	(*curr_page);
}

void FileCacheManager::rp_fifo(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame) {

	page_id page_to_evict = CS_MEM_VAL_BAD;
	frame_id frame_to_evict = CS_MEM_VAL_BAD;

	if (fifo_cached_pages.size() > CS_FIFO_THRESH_DEFAULT) {

		page_to_evict = fifo_cached_pages.back()->get();

		fifo_cached_pages.back()->erase();

		frame_to_evict = page_frame_map[page_to_evict];

		CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_ready_false();
		}
		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

	} else if (lru_cached_pages.size() > CS_LRU_THRESH_DEFAULT) {

		Frame *f = frames.operator[](page_frame_map.operator[](lru_cached_pages.back()->get()));
		DescriptorInfo *d = files.operator[](lru_cached_pages.back()->get() >> 40);

		if (step - Frame::MetaRead(f, d->meta_lock).get_last_use() > CS_LRU_THRESH_DEFAULT) {

			page_to_evict = (rng.randi() % 2) ? lru_cached_pages.back()->get() : lru_cached_pages.back()->prev()->get();

			lru_cached_pages.erase(page_to_evict);

			frame_to_evict = page_frame_map[page_to_evict];

			CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

			{
				Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
				if (w.get_dirty()) {
					enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
				}
				w.set_used(true).set_last_use(step).set_ready_false();
			}

			// We reuse the page holder we evicted.
			*curr_frame = frame_to_evict;
		}
	} else if (fifo_cached_pages.size() > CS_FIFO_THRESH_DEFAULT / 4) {

		page_to_evict = fifo_cached_pages.back()->get();

		fifo_cached_pages.back()->erase();

		frame_to_evict = page_frame_map[page_to_evict];

		CRASH_COND(frame_to_evict == CS_MEM_VAL_BAD);

		{
			Frame::MetaWrite w(frames[frame_to_evict], files[page_to_evict >> 40]->meta_lock);
			if (w.get_dirty()) {
				enqueue_store(files[page_to_evict >> 40], frame_to_evict, CS_GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			}
			w.set_used(true).set_ready_false();
		}
		// We reuse the page holder we evicted.
		*curr_frame = frame_to_evict;

	} else {
		ERR_PRINTS("CANNOT ADD FIFO PAGE TO CACHE; INSUFFICIENT SPACE.")
		CRASH_NOW()
	}

	WARN_PRINTS("evicted page under FIFO " + itoh(page_to_evict));
	// get the guid prefix and shift it to lsb to get the RID key.
	files[(page_to_evict >> 40)]->pages.erase(page_to_evict);
	page_frame_map.erase(page_to_evict);
	CRASH_COND(page_frame_map.insert(*curr_page, *curr_frame) == NULL);
	CS_GET_CACHE_POLICY_FN(cache_insertion_policies, desc_info->cache_policy)
	(*curr_page);
}

bool FileCacheManager::get_or_do_page_op(DescriptorInfo *desc_info, size_t offset) {

	page_id curr_page = get_page_guid(desc_info, offset, true);
	WARN_PRINTS("query for offset " + itoh(offset) + " : " + itoh(curr_page));
	frame_id curr_frame = CS_MEM_VAL_BAD;
	bool ret;

	if (curr_page == CS_MEM_VAL_BAD) {

		curr_page = get_page_guid(desc_info, offset, false);
		WARN_PRINT(("result of query is: " + itoh(curr_page)).utf8().get_data());

		// Find a free frame.
		for (int i = 0; i < CS_NUM_FRAMES; ++i) {

			// TODO: change this to something more efficient.
			if (!frames[i]->used) {
				Frame::MetaWrite(
						frames[i], desc_info->meta_lock)
						.set_used(true)
						.set_last_use(step)
						.set_ready_false();
				curr_frame = i;

				CRASH_COND(curr_frame == CS_MEM_VAL_BAD);
				CRASH_COND(page_frame_map.insert(curr_page, curr_frame) == NULL);

				WARN_PRINTS(itoh(curr_page) + " mapped to " + itoh(curr_frame));
				CS_GET_CACHE_POLICY_FN(
						cache_insertion_policies,
						desc_info->cache_policy)
				(curr_page);
				break;
			}
		}

		// If there are no free frames, we evict an old one according to the paging/caching algo (TODO).
		if (curr_frame == (data_descriptor)CS_MEM_VAL_BAD) {
			WARN_PRINT("must evict");
			// Evict other page somehow...
			// Remove prev page-frame mappings and associated pages.

			WARN_PRINTS("Cache policy: " + String(Dictionary(desc_info->to_variant(*this)).get("cache_policy", "-1")));

			CS_GET_CACHE_POLICY_FN(cache_replacement_policies, desc_info->cache_policy)
			(desc_info, &curr_page, &curr_frame);

			WARN_PRINTS("curr_page : " + itoh(curr_page) + " mapped to curr_frame: " + itoh(curr_frame));
		}

		desc_info->pages.ordered_insert(curr_page);

		ret = false;

	} else {
		// Update cache related details...
		CS_GET_CACHE_POLICY_FN(cache_update_policies, desc_info->cache_policy)
		(curr_page);
		ret = true;
	}

	{
		MutexLock ml(mutex);
		step += 1;
	}

	return ret;
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
	FileCacheManager &fcs = *static_cast<FileCacheManager *>(p_udata);

	do {

		ERR_PRINTS("Thread" + itoh(fcs.thread->get_id()) + "Waiting for message.");
		CtrlOp l = fcs.op_queue.pop();
		ERR_PRINT("got message");
		if (l.type == CtrlOp::QUIT) break;
		// ERR_CONTINUE(l.di == NULL);
		CRASH_COND(l.di == NULL);

		page_id curr_page = get_page_guid(l.di, l.offset, false);
		frame_id curr_frame = fcs.page_frame_map[curr_page];

		switch (l.type) {
			case CtrlOp::LOAD: {
				ERR_PRINT(("Performing load for offset " + itoh(l.offset) + "\nIn pages: " + itoh(CS_GET_PAGE(l.offset)) + "\nCurr page: " + itoh(curr_page) + "\nCurr frame: " + itoh(curr_frame)).utf8().get_data());
				fcs.do_load_op(l.di, curr_page, curr_frame, l.offset);
				break;
			}
			case CtrlOp::STORE: {
				ERR_PRINT("Performing store.");
				fcs.do_store_op(l.di, curr_page, curr_frame, l.offset);
				break;
			}
			default: CRASH_NOW();
		}
	} while (!fcs.exit_thread);
}

void FileCacheManager::check_cache(const RID rid, size_t length) {

	DescriptorInfo *desc_info = files[RID_REF_TO_DD];
	if (length == CS_LEN_UNSPECIFIED) length = 8 * CS_PAGE_SIZE;

	for (int i = CS_GET_PAGE(desc_info->offset); i < CS_GET_PAGE(desc_info->offset + length) + CS_PAGE_SIZE; i += CS_PAGE_SIZE) {
		WARN_PRINTS("curr offset for check_cache: " + itoh(i));
		if (!get_or_do_page_op(desc_info, i)) {
			// TODO: reduce inconsistency here.
			enqueue_load(desc_info, page_frame_map[desc_info->guid_prefix | i], i);
		}
	}
}

_FileCacheManager::_FileCacheManager() {
	singleton = this;
}

_FileCacheManager *_FileCacheManager::get_singleton() {
	return singleton;
}
