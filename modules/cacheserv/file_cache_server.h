/*************************************************************************/
/*  file_cache_server.h                                                  */
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

#ifndef FILE_CACHE_SERVER_H
#define FILE_CACHE_SERVER_H

#include "core/error_macros.h"
#include "core/object.h"
#include "core/os/mutex.h"
#include "core/os/thread.h"
#include "core/rid.h"
#include "core/set.h"
#include "core/variant.h"
#include "core/vector.h"

#include "cacheserv_defines.h"
#include "control_queue.h"
#include "page_table.h"

/**
 *  A page is identified with a 64 bit GUID where the 24 most significant bits act as the
 *  differenciator. The 40 least significant bits represent the offset of the referred page
 *  in its associated data source.
 *
 *  For example-
 *
 * 	mask: 0x000000FFFFFFFFFF
 *  GUID: 0x21D30E000000401D
 *
 *  Here, the offset that this page GUID refers to is 0x401D.
 *  Its range offset is 0x21D30E0000000000.
 *
 *  This lets us distinguish between pages associated with different data sources.
 */

// Get the GUID of the current offset.
// We can either get the GUID associated with an offset for the particular data source,
// or query whether a page with that GUID is currently tracked.
//
// Returns-
// The GUID, if we are not making a query, or if the page at this offset is already tracked.
// CS_MEM_VAL_BAD if we are making a query and the current page is not tracked.
_FORCE_INLINE_ page_id get_page_guid(const DescriptorInfo &di, size_t offset, bool query) {
	page_id x = di.range_offset | CS_GET_PAGE(offset);
	if (query && di.pages.find(x) == CS_MEM_VAL_BAD) {
		return CS_MEM_VAL_BAD;
	}
	return x;
}

class FileCacheServer : public Object {
	GDCLASS(FileCacheServer, Object);

	static FileCacheServer *singleton;
	bool exit_thread;
	PageTable page_table;
	RID_Owner<CachedResourceHandle> handle_owner;
	HashMap<uint32_t, DescriptorInfo *> files;
	CtrlQueue op_queue;
	Thread *thread, *th2;
	Thread *io_thread;
	Mutex *mutex;

private:
	static void thread_func(void *p_udata);
	static void th2_fn(void *p_udata);
	data_descriptor get_new_data_descriptor();
	data_descriptor add_data_source(RID *rid, FileAccess *data_source);
	void remove_data_source(data_descriptor dd);

	bool check_incomplete_nonfinal_page_load(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t extra_offset);
	bool check_incomplete_nonfinal_page_store(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame);
	void do_load_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t extra_offset = 0);
	void do_paging_op(DescriptorInfo *desc_info, page_id curr_page, frame_id *curr_frame, size_t extra_offset = 0);
	void do_store_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame);

	// Returns true if the page at the current offset is already tracked.
	// Adds the current page to the tracked list, maps it to a frame and returns false if not.
	bool check_with_page_op(DescriptorInfo *desc_info, size_t offset);

	// Returns true if the page at the current offset is already tracked.
	// Adds the current page to the tracked list, maps it to a frame and returns false if not.
	// Also sets the values of the given page and frame id args.
	bool check_with_page_op_and_update(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame, size_t offset);

protected:
public:
	FileCacheServer();
	~FileCacheServer();

	size_t read(const RID *const rid, void *const buffer, size_t length);
	size_t write(const RID *const rid, const void *const data, size_t length);
	size_t seek(const RID *const rid, size_t new_offset, int mode);

	Variant _get_state() {

		List<uint32_t> keys;
		files.get_key_list(&keys);
		Dictionary d;
		for (List<uint32_t>::Element *i = keys.front(); i; i = i->next()) {

			d[files[i->get()]->internal_data_source->get_path()] = files[i->get()]->to_variant(page_table);
		}

		return Variant(d);
	}

	static FileCacheServer *get_singleton();

	Error init();

	// Checks that all required pages are loaded and enqueues uncached pages for loading.
	void check_cache(const RID *const rid, size_t length);

	bool is_open() const; ///< true when file is open

	String get_path(const RID *const rid) const; /// returns the path for the current open file
	String get_path_absolute(const RID *const rid); /// returns the absolute path for the current open file

	void seek(const RID *const rid, size_t p_position) { seek(rid, p_position, SEEK_SET); } ///< seek to a given position
	void seek_end(const RID *const rid, int64_t p_position) { seek(rid, p_position, SEEK_END); } ///< seek from the end of file
	size_t get_position(const RID *const rid) const { return files[rid->get_id()]->offset; } ///< get position in the file
	size_t get_len(const RID *const rid) const; ///< get size of the file

	bool eof_reached(const RID *const rid) const; ///< reading passed EOF

	void flush(const RID *const rid) {
		data_descriptor dd = rid->get_id();
		DescriptorInfo *desc_info = files[dd];
		for (int i = 0; i < desc_info->total_size; i += CS_PAGE_SIZE) {
			enqueue_store(desc_info, CS_GET_PAGE(i));
		}
	}

	bool file_exists(const String &p_name) const; ///< return true if a file exists

	// uint64_t _get_modified_time(const String &p_file) { return 0; }

	// Error _chmod(const String &p_path, int p_mod) { return ERR_UNAVAILABLE; }

	// Error reopen(const String &p_path, int p_mode_flags); ///< does not change the AccessType

	// Returns an RID to an opened file.
	RID open(const String &path, int p_mode) {
		printf("%s %d\n", path.utf8().get_data(), p_mode);
		ERR_FAIL_COND_V(!mutex, RID());
		MutexLock ml = MutexLock(mutex);
		ERR_FAIL_COND_V(path.empty(), RID());

		// Will be freed when close is called with the corresponding RID.
		CachedResourceHandle *hdl = memnew(CachedResourceHandle);
		RID rid = handle_owner.make_rid(hdl);
		ERR_FAIL_COND_V(!rid.is_valid(), RID());
		FileAccess *fa = FileAccess::open(path, p_mode);
		add_data_source(&rid, fa);
		printf("open file %s with mode %d\nGot RID %d\n", path.utf8().get_data(), p_mode, rid.get_id());
		return rid;
	}

	// Invalidates the pointer. The pointer *will not* be valid after a call to this function.
	void close(RID rid) {
		printf("close file with RID %d\n", rid.get_id());
		MutexLock ml = MutexLock(mutex);
		handle_owner.free(rid);
		memfree(rid.get_data());
	}
	// Expects that the page at the given offset is in the cache.
	void enqueue_load(DescriptorInfo *desc_info, size_t offset) {
		op_queue.push(CtrlOp(desc_info, offset, CtrlOp::LOAD));
	}

	// Expects that the page at the given offset is in the cache.
	void enqueue_store(DescriptorInfo *desc_info, size_t offset) {
		op_queue.push(CtrlOp(desc_info, offset, CtrlOp::STORE));
	}

	void lock();
	void unlock();
};

class _FileCacheServer : public Object {
	GDCLASS(_FileCacheServer, Object);

	friend class FileCacheServer;
	static _FileCacheServer *singleton;

protected:
	static void _bind_methods() {
		ClassDB::bind_method(D_METHOD("get_state"), &_FileCacheServer::get_state);
	}

public:
	_FileCacheServer();
	static _FileCacheServer *get_singleton();
	static FileCacheServer *get_sss() { return FileCacheServer::get_singleton(); }
	Variant get_state() { return FileCacheServer::get_singleton()->_get_state(); }
};

#endif // FILE_CACHE_SERVER_H
