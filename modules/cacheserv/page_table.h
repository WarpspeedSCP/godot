/*************************************************************************/
/*  page_table.h                                                          */
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

#ifndef PAGE_TABLE_H
#define PAGE_TABLE_H

#include "core/map.h"
#include "core/object.h"
#include "core/os/file_access.h"
#include "core/os/mutex.h"
#include "core/os/rw_lock.h"
#include "core/os/thread.h"
#include "core/reference.h"
#include "core/rid.h"
#include "core/set.h"
#include "core/variant.h"
#include "core/vector.h"

#include "cacheserv_defines.h"

typedef uint32_t data_descriptor;
typedef uint32_t frame_id;
typedef size_t page_id;

struct PageTable;
struct Frame;
struct DescriptorInfo;

struct Frame {
private:
	uint16_t used_size;
	uint8_t *const memory_region;
	bool dirty;
	bool recently_used;
	bool used;
	// volatile uint16_t rd_count; // Number of readers.
	// volatile uint8_t wr_lock; // When a write lock on this frame is acquired, this
	RWLock *rwl;

public:
	Frame() :
			memory_region(NULL),
			used_size(0),
			recently_used(false),
			used(false),
			dirty(false),
			rwl(NULL)
	// rd_count(0),
	// wr_lock(0)
	{}

	explicit Frame(
			uint8_t *i_memory_region) :

			memory_region(i_memory_region),
			used_size(0),
			recently_used(false),
			used(false),
			dirty(false),
			rwl(RWLock::create())
	// rd_count(0),
	// wr_lock(0)
	{}

	Variant to_variant() const {
		Dictionary a;
		a["memory_region"] = Variant(String((char *)memory_region));
		a["used_size"] = Variant((int)used_size);
		a["recently_used"] = Variant(recently_used);
		a["used"] = Variant(used);
		a["dirty"] = Variant(dirty);

		return Variant(a);
	}

	~Frame() { memdelete(rwl); }

	class Read {
	private:
		const Frame *alloc;
		RWLock *rwl;
		const uint8_t *mem;
		Error valid;

	public:
		_FORCE_INLINE_ const uint8_t &operator[](int p_index) const { return mem[p_index]; }
		_FORCE_INLINE_ const uint8_t *ptr() const { return mem; }

		_FORCE_INLINE_ uint16_t get_used_size() {
			return alloc->used_size;
		}

		_FORCE_INLINE_ bool get_dirty() {
			return alloc->dirty;
		}

		_FORCE_INLINE_ bool get_used() {
			return alloc->used;
		}

		_FORCE_INLINE_ bool get_recently_used() {
			return alloc->recently_used;
		}

		bool is_valid() { return valid == OK; }

		void acquire() {
			if (valid != OK) {
				WARN_PRINT("Acquiring lock.");
				rwl->read_lock();
				valid = OK;
			}
		}

		Read &operator=(const Read &other) {
			if (rwl == other.rwl)
				return *this;

			rwl->read_unlock();

			rwl = other.rwl;

			WARN_PRINT("Trying lock.");
			valid = rwl->read_try_lock();
			WARN_PRINT(("Got result " + itos(valid) + " after lock.").utf8().get_data());

			mem = other.mem;

			return *this;
		}

		Read &operator=(const Frame *const frame) {
			if (rwl == frame->rwl)
				return *this;

			rwl->read_unlock();

			rwl = frame->rwl;

			WARN_PRINT("Trying lock.");
			valid = rwl->read_try_lock();
			WARN_PRINT(("Got result " + itos(valid) + " after lock.").utf8().get_data());

			mem = frame->memory_region;

			return *this;
		}

		Read() :
				alloc(NULL),
				rwl(NULL),
				mem(NULL) {}

		explicit Read(const Frame *alloc) :
				alloc(alloc),
				rwl(alloc->rwl),
				mem(alloc->memory_region) {
			valid = rwl->read_try_lock();
		}

		explicit Read(const Read &other) :
				rwl(other.rwl),
				mem(other.mem) { valid = rwl->read_try_lock(); }

		~Read() {
			if (rwl) {
				rwl->read_unlock();
				WARN_PRINT("Released lock.");
			}

		}
	};

	class Write {
	private:
		Frame *alloc;
		RWLock *rwl;
		uint8_t *mem;
		Error valid;

	public:
		_FORCE_INLINE_ uint8_t &operator[](int p_index) const { return mem[p_index]; }
		_FORCE_INLINE_ uint8_t *ptr() const { return mem; }

		_FORCE_INLINE_ uint16_t get_used_size() {
			return alloc->used_size;
		}

		_FORCE_INLINE_ void set_used_size(uint16_t in) {
			alloc->used_size = in;
		}

		_FORCE_INLINE_ bool get_dirty() {
			return alloc->dirty;
		}

		_FORCE_INLINE_ void set_dirty(bool in) {
			alloc->dirty = in;
		}

		_FORCE_INLINE_ bool get_used() {
			return alloc->used;
		}

		_FORCE_INLINE_ void set_used(bool in) {
			alloc->used = in;
		}

		_FORCE_INLINE_ bool get_recently_used() {
			return alloc->recently_used;
		}

		_FORCE_INLINE_ void set_recently_used(bool in) {
			alloc->recently_used = in;
		}

		bool is_valid() { return valid == OK; }

		void acquire() {
			if (valid != OK) {
				WARN_PRINT("Acquiring lock.");
				rwl->write_lock();
				valid = OK;
			}
		}

		Frame *operator->() {
			return alloc;
		}

		Write &operator=(const Write &p_read) {
			if (rwl == p_read.rwl)
				return *this;

			rwl->write_unlock();

			rwl = p_read.rwl;

			WARN_PRINT("Trying lock.");
			valid = rwl->write_try_lock();
			WARN_PRINT(("Got result " + itos(valid) + " after lock.").utf8().get_data());

			mem = p_read.mem;

			return *this;
		}

		Write &operator=(Frame *const p_read) {
			if (rwl == p_read->rwl)
				return *this;

			rwl->write_unlock();

			rwl = p_read->rwl;

			WARN_PRINT("Trying lock.");
			valid = rwl->write_try_lock();
			WARN_PRINT(("Got result " + itos(valid) + " after lock.").utf8().get_data());

			mem = p_read->memory_region;

			alloc = p_read;

			return *this;
		}

		Write() :
				rwl(NULL),
				mem(NULL),
				valid(ERR_LOCKED) {}

		explicit Write(Frame *const p_alloc) :
				alloc(p_alloc),
				rwl(p_alloc->rwl),
				mem(p_alloc->memory_region) {
			WARN_PRINT("Trying lock.");
			valid = rwl->write_try_lock();
			WARN_PRINT(("Got result " + itos(valid) + " after lock.").utf8().get_data());
		}

		explicit Write(Write &other) :
				alloc(other.alloc),
				rwl(other.rwl),
				mem(other.mem) { valid = rwl->write_try_lock(); }

		~Write() {
			if (rwl) {
				rwl->write_unlock();
				WARN_PRINT("Released lock.");
			}
		}
	};
};

struct DescriptorInfo {
	size_t offset;
	size_t total_size;
	size_t range_offset;
	Vector<page_id> pages;
	FileAccess *internal_data_source;

	// Create a new DescriptorInfo with a new random namespace defined by 24 most significant bits.
	explicit DescriptorInfo(FileAccess *fa, page_id new_range);
	~DescriptorInfo(){};

	Variant to_variant(const PageTable &p);
};

struct PageTable {
	Set<page_id> ranges = Set<size_t>();
	Vector<page_id> pages;
	Vector<Frame *> frames;
	Map<page_id, frame_id> page_frame_map;
	uint8_t *memory_region = NULL;
	size_t available_space;
	size_t used_space;
	size_t total_space;
};

#endif // !PAGE_TABLE_H
