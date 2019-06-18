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
#include "core/os/thread.h"
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
	uint8_t *memory_region;
	uint16_t used_size;
	bool recently_used;
	bool used;
	bool dirty;
	volatile uint16_t rd_count;
	volatile uint16_t wr_count;

	Frame() :
		memory_region(),
		used_size(0),
		recently_used(false),
		used(false),
		dirty(false)
		 {
			 m = Mutex::create();
		 }

	explicit Frame(
			uint8_t *i_memory_region
		) :

			memory_region(i_memory_region),
			used_size(0),
			recently_used(false),
			used(false),
			dirty(false)
			{
				m = Mutex::create();
				m->unlock();
			}

	~Frame() { memdelete(m); }

	Variant to_variant() const {
		Dictionary a;
		a["memory_region"] = Variant(String((char *)memory_region));
		a["used_size"] = Variant((int)used_size);
		a["recently_used"] = Variant(recently_used);
		a["used"] = Variant(used);
		a["dirty"] = Variant(dirty);

		return Variant(a);
	}

	// void lock() {
	// 	atomic_increment(&lock);
	// }

	// void unlock() {
	// 	if(lock > 0)
	// 		atomic_decrement(&lock);
	// }
};

struct DescriptorInfo {
	size_t offset;
	size_t total_size;
	size_t range_offset;
	Vector<page_id> pages;
	FileAccess *internal_data_source;

	// Create a new DescriptorInfo with a new random namespace defined by 24 most significant bits.
	explicit DescriptorInfo(FileAccess *fa, page_id new_range);
	~DescriptorInfo() {};

	Variant to_variant(const PageTable &p);
};


struct PageTable {
	Set<page_id> ranges = Set<size_t>();
	Vector<page_id> pages;
	Vector<Frame> frames;
	Map<page_id, frame_id> page_frame_map;
	uint8_t *memory_region = NULL;
	size_t available_space;
	size_t used_space;
	size_t total_space;
};

#endif // !PAGE_TABLE_H
