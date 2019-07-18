/*************************************************************************/
/*  cache_info_table.h                                                   */
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

#ifndef CACHE_INFO_TABLE_H
#define CACHE_INFO_TABLE_H

#include "core/map.h"
#include "core/object.h"
#include "core/os/file_access.h"
#include "core/os/semaphore.h"
#include "core/os/rw_lock.h"
#include "core/os/thread.h"
#include "core/reference.h"
#include "core/rid.h"
#include "core/set.h"
#include "core/variant.h"
#include "core/vector.h"

#include "cacheserv_defines.h"

_FORCE_INLINE_ String itoh(size_t num) {
	char x[100];
	snprintf(x, 100,"%lx\0", (unsigned long)num);
	return String(x);
}

typedef uint32_t data_descriptor;
typedef uint32_t frame_id;
typedef size_t page_id;

struct CacheInfoTable;
struct Frame;
struct DescriptorInfo;

class FileCacheManager;

struct Frame {
	friend class FileCacheManager;

private:
	uint8_t *const memory_region;
	uint32_t ts_last_use;
	uint16_t used_size;
	bool dirty;
	volatile bool ready;
	volatile bool used;

public:
	Frame() :
			memory_region(NULL),
			used_size(0),
			ts_last_use(0),
			used(false),
			dirty(false),
			ready(false)
	// rd_count(0),
	// wr_lock(0)
	{}

	explicit Frame(
			uint8_t *i_memory_region) :

			memory_region(i_memory_region),
			used_size(0),
			ts_last_use(0),
			used(false),
			dirty(false),
			ready(false)
	// rd_count(0),
	// wr_lock(0)
	{}

	~Frame() {

	}

	Variant to_variant() const {
		Dictionary a;
		String s = String((char *)memory_region);
		s.resize(100);
		a["memory_region"] = Variant(" ... " + s + " ... ");
		a["used_size"] = Variant(itoh(used_size));
		a["time_since_last_use"] = Variant(itoh(ts_last_use));
		a["used"] = Variant(used);
		a["dirty"] = Variant(dirty);

		return Variant(a);
	}


	class MetaRead {
	private:
		const Frame *alloc;
		RWLock *rwl;

	public:
		_FORCE_INLINE_ uint16_t get_used_size() {
			return alloc->used_size;
		}

		_FORCE_INLINE_ bool get_dirty() {
			return alloc->dirty;
		}

		_FORCE_INLINE_ bool get_used() {
			return alloc->used;
		}

		_FORCE_INLINE_ uint32_t get_last_use() {
			return alloc->ts_last_use;
		}

		_FORCE_INLINE_ bool get_ready() {
			return alloc->ready;
		}

		void acquire() {
			// WARN_PRINT(("Acquiring metadata READ lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			rwl->read_lock();
		}

		MetaRead() :
				alloc(NULL),
				rwl(NULL) {}

		MetaRead(const Frame *alloc, RWLock *meta_lock) :
				alloc(alloc),
				rwl(meta_lock) {
			acquire();
		}

		~MetaRead() {
			if (rwl) {
				rwl->read_unlock();
				// WARN_PRINT(("Released metadata READ lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			}
		}
	};

	class DataRead {
	private:
		RWLock *rwl;
		const uint8_t *mem;

	public:
		_FORCE_INLINE_ const uint8_t &operator[](int p_index) const { return mem[p_index]; }
		_FORCE_INLINE_ const uint8_t *ptr() const { return mem; }

		void acquire() {
			rwl->read_lock();
		}

		DataRead() :
				rwl(NULL),
				mem(NULL) {}

		DataRead(const Frame *alloc, Semaphore *ready_sem, RWLock *data_lock) :
				rwl(data_lock),
				mem(alloc->memory_region) {
			while (!(alloc->ready)) ready_sem->wait();
			// WARN_PRINT(("Acquiring data READ lock in thread ID "  + itoh(Thread::get_caller_id()) ).utf8().get_data());
			acquire();
		}

		~DataRead() {
			if (rwl) {
				rwl->read_unlock();
				// WARN_PRINT(("Releasing data READ lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			}
		}
	};

	class MetaWrite {
	private:
		Frame *alloc;
		RWLock *rwl;

	public:
		_FORCE_INLINE_ uint16_t get_used_size() {
			return alloc->used_size;
		}

		_FORCE_INLINE_ MetaWrite &set_used_size(uint16_t in) {
			alloc->used_size = in;
			return *this;
		}

		_FORCE_INLINE_ bool get_dirty() {
			return alloc->dirty;
		}

		_FORCE_INLINE_ MetaWrite &set_dirty_true(Semaphore *dirty_sem) {
			alloc->dirty = true;
			WARN_PRINT("Dirty page written to disk.");
			dirty_sem->post();
			return *this;
		}

		_FORCE_INLINE_ MetaWrite &set_dirty_false(Semaphore *dirty_sem) {
			alloc->dirty = false;
			WARN_PRINT("Dirty page written to disk.");
			dirty_sem->post();
			return *this;
		}

		_FORCE_INLINE_ bool get_used() {
			return alloc->used;
		}

		_FORCE_INLINE_ MetaWrite &set_used(bool in) {
			alloc->used = in;
			return *this;
		}

		_FORCE_INLINE_ bool get_ready() {
			return alloc->ready;
		}

		_FORCE_INLINE_ MetaWrite &set_ready_true(Semaphore *ready_sem, page_id page, frame_id frame) {
			alloc->ready = true;
			WARN_PRINTS("Part ready for page " + itoh(page) + " and frame " + itoh(frame) + " .");
			ready_sem->post();
			return *this;
		}

		_FORCE_INLINE_ MetaWrite &set_ready_false() {
			alloc->ready = false;
			WARN_PRINT("Part not ready.");
			return *this;
		}

		_FORCE_INLINE_ uint32_t get_last_use() {
			return alloc->ts_last_use;
		}

		_FORCE_INLINE_ MetaWrite &set_last_use(uint32_t in) {
			alloc->ts_last_use = in;
			return *this;
		}

		void acquire() {
			// WARN_PRINT(("Acquiring metadata WRITE lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			rwl->write_lock();
		}

		Frame *operator->() {
			return alloc;
		}

		MetaWrite() :
				alloc(NULL),
				rwl(NULL) {}

		MetaWrite(Frame *const alloc, RWLock *meta_lock) :
				alloc(alloc),
				rwl(meta_lock) {
			acquire();
		}

		~MetaWrite() {
			if (rwl) {
				rwl->write_unlock();
				// WARN_PRINT(("Releasing metadata WRITE lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			}
		}
	};

	class DataWrite {
	private:
		RWLock *rwl;
		uint8_t *mem;

	public:
		_FORCE_INLINE_ uint8_t &operator[](int p_index) const { return mem[p_index]; }
		_FORCE_INLINE_ uint8_t *ptr() const { return mem; }

		void acquire() {
			// WARN_PRINT(("Acquiring data WRITE lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			rwl->write_lock();
		}

		DataWrite() :
				rwl(NULL),
				mem(NULL) {}

		DataWrite(Frame *const p_alloc, Semaphore *dirty_sem, RWLock *data_lock) :
				rwl(data_lock),
				mem(p_alloc->memory_region) {
			while ( p_alloc->dirty ) dirty_sem->wait();
			acquire();
		}

		~DataWrite() {
			if (rwl) {
				rwl->write_unlock();
				// WARN_PRINT(("Releasing data WRITE lock in thread ID " + itoh(Thread::get_caller_id())).utf8().get_data());
			}
		}
	};
};

struct DescriptorInfo {
	size_t offset;
	size_t total_size;
	size_t guid_prefix;
	Vector<page_id> pages;
	String abs_path;
	int cache_policy;
	int max_pages;
	FileAccess *internal_data_source;
	Semaphore *sem;
	RWLock *meta_lock;
	RWLock *data_lock;
	bool valid;

	// Create a new DescriptorInfo with a new random namespace defined by 24 most significant bits.
	DescriptorInfo(FileAccess *fa, page_id new_guid_prefix, int cache_policy);
	~DescriptorInfo() {
		memdelete(sem);
		memdelete(meta_lock);
		memdelete(data_lock);
	}

	Variant to_variant(const FileCacheManager &p);
};

#endif // !CACHE_INFO_TABLE_H
