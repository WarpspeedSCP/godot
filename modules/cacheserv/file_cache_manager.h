/*************************************************************************/
/*  file_cache_manager.h                                                 */
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
/* MERCHANTABILITY, FITNESS FOR A PAGEICULAR PURPOSE AND NONINFRINGEMENT.*/
/* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY  */
/* CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,  */
/* TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE     */
/* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.                */
/*************************************************************************/

#ifndef FILE_CACHE_MANAGER_H
#define FILE_CACHE_MANAGER_H

#include "core/error_macros.h"
#include "core/math/random_number_generator.h"
#include "core/object.h"
#include "core/ordered_hash_map.h"
#include "core/os/mutex.h"
#include "core/os/thread.h"
#include "core/rid.h"
#include "core/set.h"
#include "core/variant.h"
#include "core/vector.h"

#include "cache_info_table.h"
#include "cacheserv_defines.h"
#include "control_queue.h"

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
// We can either get the GUID associated with an offset for the pageicular data source,
// or query whether a page with that GUID is currently tracked.
//
// Returns-
// The GUID, if we are not making a query, or if the page at this offset is already tracked.
// CS_MEM_VAL_BAD if we are making a query and the current page is not tracked.
_FORCE_INLINE_ page_id get_page_guid(const DescriptorInfo *di, size_t offset, bool query) {
	page_id x = di->guid_prefix | CS_GET_PAGE(offset);
	if (query && di->pages.find(x) == CS_MEM_VAL_BAD) {
		return CS_MEM_VAL_BAD;
	}
	return x;
}

struct LRUComparator;

class FileCacheManager : public Object {
	GDCLASS(FileCacheManager, Object);

	friend class _FileCacheManager;

	static FileCacheManager *singleton;
	RandomNumberGenerator rng;
	bool exit_thread;
	RID_Owner<CachedResourceHandle> handle_owner;
	CtrlQueue op_queue;
	Thread *thread, *th2;
	Mutex *mutex;

public:
	Vector<Frame *> frames;
	HashMap<uint32_t, DescriptorInfo *> files;
	Map<page_id, frame_id> page_frame_map;
	Set<page_id, LRUComparator> lru_cached_pages;
	List<page_id> fifo_cached_pages;
	Set<page_id, LRUComparator> permanent_cached_pages;

	uint8_t *memory_region = NULL;
	uint64_t step = 0;
	size_t available_space;
	size_t used_space;
	size_t total_space;

private:
	static void thread_func(void *p_udata);

	data_descriptor add_data_source(RID *rid, FileAccess *data_source, int cache_policy);
	void remove_data_source(data_descriptor dd);

	void enforce_cache_policy(DescriptorInfo *desc_info, page_id curr_page);
	bool check_incomplete_page_load(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset);
	bool check_incomplete_page_store(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset);
	void do_load_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset);
	void do_paging_op(DescriptorInfo *desc_info, page_id curr_page, frame_id *curr_frame, size_t offset = 0UL);
	void do_store_op(DescriptorInfo *desc_info, page_id curr_page, frame_id curr_frame, size_t offset);

	// Returns true if the page at the current offset is already tracked.
	// Adds the current page to the tracked list, maps it to a frame and returns false if not.
	// Also sets the values of the given page and frame id args.
	bool check_with_page_op(DescriptorInfo *desc_info, size_t offset, page_id *curr_page, frame_id *curr_frame);

protected:
public:

	enum CachePolicy {
		KEEP,
		LRU,
		FIFO
	};

	typedef void (FileCacheManager::*replacement_policy_fn)(DescriptorInfo *, page_id *, frame_id *);
	typedef void (FileCacheManager::*insertion_policy_fn)(page_id);

	void rp_lru(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame);
	void rp_fifo(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame);
	void rp_keep(DescriptorInfo *desc_info, page_id *curr_page, frame_id *curr_frame);

	void ip_lru (page_id curr_page) {
		WARN_PRINT("LRU cached.");
		lru_cached_pages.insert(curr_page);
	}
	void ip_fifo(page_id curr_page) {
		WARN_PRINT("FIFO cached.");
		fifo_cached_pages.push_front(curr_page);
	}
	void ip_keep(page_id curr_page) {
		WARN_PRINT("Permanent cached.");
		permanent_cached_pages.insert(curr_page);
	}

	insertion_policy_fn cache_insertion_policies[3] = {
		&FileCacheManager::ip_keep,
		&FileCacheManager::ip_lru,
		&FileCacheManager::ip_fifo
	};

	replacement_policy_fn cache_replacement_policies[3] = {
		&FileCacheManager::rp_keep,
		&FileCacheManager::rp_lru,
		&FileCacheManager::rp_fifo
	};

	FileCacheManager();
	~FileCacheManager();

	size_t read(const RID *const rid, void *const buffer, size_t length);
	size_t write(const RID *const rid, const void *const data, size_t length);
	size_t seek(const RID *const rid, size_t new_offset, int mode);

	Variant _get_state() {

		List<uint32_t> keys;
		files.get_key_list(&keys);
		Dictionary d;
		for (List<uint32_t>::Element *i = keys.front(); i; i = i->next()) {

			d[files[i->get()]->internal_data_source->get_path()] = files[i->get()]->to_variant(*this);
		}

		return Variant(d);
	}

	static FileCacheManager *get_singleton();

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

	// Flush cache to disk.
	void flush(const RID *const rid);

	bool file_exists(const String &p_name) const; ///< return true if a file exists

	// uint64_t _get_modified_time(const String &p_file) { return 0; }

	// Error _chmod(const String &p_path, int p_mod) { return ERR_UNAVAILABLE; }

	// Error reopen(const String &p_path, int p_mode_flags); ///< does not change the AccessType

	// Returns an RID to an opened file.
	RID open(const String &path, int p_mode, int cache_policy);

	// Close the file but keep its contents in the cache. None of the state information is invalidated.
	void close(const RID *const rid);

	// Invalidates the RID. it *will not* be valid after a call to this function.
	void permanent_close(const RID *const rid);

	Error reopen(const RID *const rid, int mode);

	// Expects that the page at the given offset is in the cache.
	void enqueue_load(DescriptorInfo *desc_info, frame_id curr_frame, size_t offset) {
		op_queue.push(CtrlOp(desc_info, curr_frame, offset, CtrlOp::LOAD));
	}

	// Expects that the page at the given offset is in the cache.
	void enqueue_store(DescriptorInfo *desc_info, frame_id curr_frame, size_t offset) {
		op_queue.push(CtrlOp(desc_info, curr_frame, offset, CtrlOp::STORE));
	}

	void lock();
	void unlock();
};


class _FileCacheManager : public Object {
	GDCLASS(_FileCacheManager, Object);

	friend class FileCacheManager;
	static _FileCacheManager *singleton;

protected:
	static void _bind_methods() {
		ClassDB::bind_method(D_METHOD("get_state"), &_FileCacheManager::get_state);
		BIND_ENUM_CONSTANT(KEEP);
		BIND_ENUM_CONSTANT(LRU);
		BIND_ENUM_CONSTANT(FIFO);
	}

public:

	enum CachePolicy {
		KEEP,
		LRU,
		FIFO
	};

	_FileCacheManager();
	static FileCacheManager *get_sss() { return FileCacheManager::get_singleton(); }
	static _FileCacheManager *get_singleton();
	Variant get_state() { return FileCacheManager::get_singleton()->_get_state(); }
};

VARIANT_ENUM_CAST(_FileCacheManager::CachePolicy);

struct LRUComparator {
	const FileCacheManager *const fcm;
	LRUComparator () : fcm(_FileCacheManager::get_sss()) {}
	_FORCE_INLINE_ bool operator()(page_id p1, page_id p2) {
		size_t a = Frame::MetaRead(
			fcm->frames.operator[](fcm->page_frame_map[p1]),
			fcm->files.operator[]((data_descriptor)(p1 >> 40))->meta_lock
		).get_last_use();

		size_t b = Frame::MetaRead(
			fcm->frames.operator[](fcm->page_frame_map[p2]),
			fcm->files.operator[]((data_descriptor)(p2 >> 40))->meta_lock
		).get_last_use();
		bool x = a > b;

		// WARN_PRINT((itoh(p1) + (x ? " is older than " : " is younger than ") + itoh(p2)).utf8().get_data());
		// WARN_PRINT(("age of p1: " + itoh(a) + " age of p2: " + itoh(b)).utf8().get_data());
		return x;
	}
};

#endif // !FILE_CACHE_MANAGER_H
