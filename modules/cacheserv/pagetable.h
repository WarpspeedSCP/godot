/*************************************************************************/
/*  pagetable.h                                                          */
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

#ifndef PAGETABLE_H
#define PAGETABLE_H

#include "core/map.h"
#include "core/object.h"
#include "core/os/mutex.h"
#include "core/os/thread.h"
#include "core/rid.h"
#include "core/set.h"
#include "core/variant.h"
#include "core/vector.h"

#include "cacheserv_defines.h"

typedef int file_descriptor;
typedef uint32_t frame_id;
typedef uint32_t page_id;


struct Frame {

	enum CachePolicy {
		KEEP_FOREVER,
		FIFO,
	};

	uint8_t *memory_region;
	uint64_t data_offset;
	CachePolicy cache_policy;
	bool recently_used;
	bool used;
	bool dirty;

	Frame() {}

	Frame(
			uint8_t *i_memory_region,
			uint64_t i_data_offset,
			CachePolicy i_cache_policy = CachePolicy::FIFO) :
			memory_region(i_memory_region),
			data_offset(i_data_offset),
			cache_policy(i_cache_policy),
			recently_used(false),
			used(false),
			dirty(false) {}
};

struct Region {
	size_t start_page_idx; // First page's index.
	size_t size; // Size in pages.
	size_t prev;
	size_t next; // In case the region is not contiguous.
	uint8_t *mem_ptr;

	Region() :
			start_page_idx(CS_MEM_VAL_BAD),
			size(0),
			prev(CS_MEM_VAL_BAD),
			next(CS_MEM_VAL_BAD),
			mem_ptr(NULL) {}

	Region(
			size_t i_start_page_idx,
			size_t i_size,
			size_t i_prev,
			size_t i_next,
			uint8_t *i_ptr) :
			start_page_idx(i_start_page_idx),
			size(i_size),
			prev(i_prev),
			next(i_next),
			mem_ptr(i_ptr) {}
};

struct PageTable {
	Vector<Frame> frames;
	Map<page_id, frame_id> page_frame_map;
	Map<file_descriptor, Vector<page_id>> file_page_map;
	uint8_t *memory_region = NULL;
	size_t available_space;
	size_t used_space;
	size_t total_space;

	PageTable();

	~PageTable();
};

#endif // !PAGETABLE_H
