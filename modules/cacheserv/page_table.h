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

typedef int data_descriptor;
typedef uint32_t frame_id;
typedef size_t page_id;



struct Frame {

	enum CachePolicy {
		KEEP_FOREVER,
		FIFO,
	};

	uint8_t *memory_region;
	CachePolicy cache_policy;
	uint16_t used_size;
	bool recently_used;
	bool used;
	bool dirty;

	Frame() {}

	Frame(
			uint8_t *i_memory_region,
			CachePolicy i_cache_policy = CachePolicy::FIFO) :
			memory_region(i_memory_region),
			cache_policy(i_cache_policy),
			recently_used(false),
			used(false),
			dirty(false) {}
};

struct DescriptorInfo {
	size_t offset;
	size_t total_size;
	size_t range_offset;
	Vector<page_id> pages;
	FileAccess *internal_data_source;

	DescriptorInfo() { internal_data_source = NULL; pages.clear(); }
	DescriptorInfo(FileAccess *fa);
	~DescriptorInfo();
};


struct PageTable {
	Vector<Frame> frames;
	Map<page_id, frame_id> page_frame_map;
	Map<data_descriptor, DescriptorInfo > file_page_map;
	uint8_t *memory_region = NULL;
	size_t available_space;
	size_t used_space;
	size_t total_space;

	PageTable();

	int get_new_data_descriptor();
	int add_data_source(FileAccess *data_source);

	size_t read(data_descriptor dd, void *buffer, size_t length);
	size_t write(data_descriptor dd, void *data, size_t length);
	size_t seek(data_descriptor dd, size_t new_offset, int mode);

	bool check_incomplete_nonfinal_page_load(DescriptorInfo &desc_info, size_t &curr_page, size_t &curr_frame, size_t extra_offset);
	void do_paging_op(DescriptorInfo &desc_info,size_t &curr_page, size_t &curr_frame, size_t extra_offset = 0);
	void do_load_op(DescriptorInfo &desc_info, size_t &curr_page, size_t &curr_frame, size_t extra_offset = 0);
	~PageTable();
};

#endif // !PAGE_TABLE_H
