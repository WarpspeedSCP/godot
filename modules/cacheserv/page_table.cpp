#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "page_table.h"

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

// The number of bytes after the previous page offset for the offset a.
#define PARTIAL_SIZE(a) (a % CS_PAGE_SIZE)
// Extract offset from GUID by masking the range.
#define GET_FILE_OFFSET_FROM_GUID(guid) (guid & 0x0000FFFFFFFFFFFF)
// Round off to the previous page offset.
#define GET_PAGE(a) (a - PARTIAL_SIZE(a))


/**
 *  A page is identified with a 64 bit GUID where the 16 most significant bits act as the
 *  differenciator. The 48 least significant bits represent the offset of the referred page
 *  in its associated data source.
 *
 *  For example-
 *
 * 	mask: 0x0000FFFFFFFFFFFF
 *  GUID: 0x21D300000000401D
 *
 *  Here, the offset that this page GUID refers to is 0x401D.
 *  Its range offset is 0x21D3000000000000.
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
_FORCE_INLINE_ page_id get_page_guid(DescriptorInfo di, size_t offset, bool query) {
	page_id x = di.range_offset | GET_PAGE(offset);
	if(query && di.pages.find(x) == CS_MEM_VAL_BAD) { return CS_MEM_VAL_BAD; }
	return x;
}


static Set<size_t> ranges;
static Vector<page_id> pages;


// Create a new DescriptorInfo with a new random namespace defined by 16 most significant bits.
DescriptorInfo::DescriptorInfo(FileAccess *fa) {

	srandom(time(0));
	size_t new_range = (size_t)random() << 48;

	while (ranges.has(new_range)) {
		new_range = (size_t)random() << 48;
	}

	range_offset = new_range;
	internal_data_source = fa;
	offset = 0;
	total_size = internal_data_source->get_len();
}


// Remove the range offset and all pages associated with the DescriptorInfo.
DescriptorInfo::~DescriptorInfo() {
	ranges.erase(range_offset);

	for(int i = 0; i < pages.size(); ++i) {
		if((pages[i] & 0xFFFF000000000000) == range_offset)
			pages.set(i, CS_MEM_VAL_BAD);
	}

	pages.sort();

	for(int i = 0; i < pages.size(); i++) {
		if(pages[i] == CS_MEM_VAL_BAD) {
			pages.resize(i);
			break;
		}
	}
}

PageTable::~PageTable() {
	if (memory_region) {
		memdelete_arr(memory_region);
	}
}

PageTable::PageTable() {

	page_frame_map.clear();
	file_page_map.clear();
	frames.clear();
	ranges.clear();
	pages.clear();

	available_space = CS_CACHE_SIZE;
	used_space = 0;
	total_space = CS_CACHE_SIZE;

	if (memory_region) {
		memdelete_arr(memory_region);
	}

	memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);

	for (size_t i = 0; i < CS_NUM_FRAMES; ++i) {
		frames.push_back(
				Frame(memory_region + i * CS_PAGE_SIZE));
	}

}

// Data descriptors are unique 16 bit IDs to represent data sources.
int PageTable::get_new_data_descriptor() {
	srandom(time(0));
	int dd = -1;
	while (file_page_map.has(dd = random() & 0x0000FFFF))
		;
	return dd;
}


int PageTable::add_data_source(FileAccess *data_source) {
	int new_dd = get_new_data_descriptor();
	file_page_map.insert(new_dd, DescriptorInfo(data_source));
	seek(new_dd, 0, SEEK_SET);
	return new_dd;
}


// !!! takes mutable references to all params.
// This operation selects a new frame, or evicts an old frame to hold a new page.
void PageTable::do_paging_op(DescriptorInfo &desc_info, size_t &curr_page, size_t &curr_frame, size_t extra_offset) {

	// Find a free frame.
	for (int i = 0; i < CS_NUM_FRAMES; ++i) {
		if (!frames[i].used) {

			Frame &f = frames.ptrw()[i];
			f.used = true;
			f.recently_used = true;
			curr_frame = i;
			break;

		}
	}

	// If there are no free frames, we evict an old one according to the paging/caching algo (TODO).
	if (curr_frame == CS_MEM_VAL_BAD) {
		// Evict other page somehow...
		// Remove prev page-frame mappings and associated pages.

		srandom(time(0));
		page_id page_to_evict = random() % pages.size();

		page_to_evict = pages[page_to_evict];

		frame_id frame_to_evict = page_frame_map[page_to_evict];

		Frame &f = frames.ptrw()[frame_to_evict];
		if (f.dirty) {
			// seek to pos in internal data source.
			// write the number of bytes that this page contains (indicated by used_size).

			desc_info.internal_data_source->seek(GET_FILE_OFFSET_FROM_GUID(page_to_evict));
			desc_info.internal_data_source->store_buffer(f.memory_region, f.used_size);
		}

		// Reset flags.
		f.dirty = false;
		f.recently_used = false;
		f.used = false;

		// Erase old info.
		page_frame_map.erase(page_to_evict);
		pages.erase(page_to_evict);

		// We reuse the frame we evicted.
		curr_frame = frame_to_evict;
		f.used = true;
		f.recently_used = true;
	}
}

// !!! takes mutable references to all params.
// Takes an extra_offset parameter that we use to keep track
// of temporary updates to the current offset in the file.
void PageTable::do_load_op(DescriptorInfo &desc_info, size_t &curr_page, size_t &curr_frame, size_t extra_offset) {

	// Get the GUID of the page at the current offset.
	curr_page = get_page_guid(desc_info, desc_info.offset + extra_offset, false);

	// Add the current page to the list of tracked pages.
	desc_info.pages.ordered_insert(curr_page);

	// Perform a paging operation.
	do_paging_op(desc_info, curr_page, curr_frame);

	// Add the new mapping generated.
	page_frame_map.insert(curr_page, curr_frame);

	{
		// Get data from data source somehow...

		if (check_incomplete_nonfinal_page_load(desc_info, curr_page, curr_frame, extra_offset)) {
			ERR_PRINT("Read less than " STRINGIFY(CS_PAGE_SIZE) " bytes.")
		} else {
		}
	}
}

// !!! takes mutable references to all params.
// The extra_offset param is used to track temporary changes to file offset.
//
//  Returns true if -
//	1. A read from the current offset returns less than CS_PAGE_SIZE bytes and,
//  2. The current page is not the last page of the file.
//
// This operation updates the used_size value of the frame.
_FORCE_INLINE_ bool PageTable::check_incomplete_nonfinal_page_load(DescriptorInfo &desc_info, size_t &curr_page, size_t &curr_frame, size_t extra_offset) {
	desc_info.internal_data_source->seek(GET_FILE_OFFSET_FROM_GUID(curr_page));
	return ((frames.ptrw()[curr_frame].used_size = desc_info.internal_data_source->get_buffer(frames[curr_frame].memory_region, CS_PAGE_SIZE)) < CS_PAGE_SIZE) && (GET_PAGE(desc_info.offset + extra_offset) < GET_PAGE(desc_info.total_size));
}

/**
 *  In both read and write operations, one invariant holds-
 *  The seek operation should have placed the offset value of
 *  the current DescriptorInfo at the correct position.
 */


// Perform a read operation.
size_t PageTable::read(data_descriptor dd, void *buffer, size_t length) {

	Map<data_descriptor, DescriptorInfo>::Element *elem = file_page_map.find(dd);

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo desc_info = elem->get();
		size_t final_partial_length = PARTIAL_SIZE(length);
		size_t curr_page = CS_MEM_VAL_BAD, curr_frame = CS_MEM_VAL_BAD, buffer_offset = 0;

		// We need to handle the first and last frames differently,
		// because the data to be copied may not start at a page boundary, and may not end on a page boundary.
		{
			// Query for the page with the current offset.
			if ((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped, do load op.
				// This will update the current page and frame.
				do_load_op(desc_info, curr_frame, curr_page, buffer_offset - desc_info.offset);
			}

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset)
			//  gives us the address of the first byte to copy which may or may not be on a page boundary.
			//
			// We can copy only CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset) which gives us the number
			//  of bytes from the current offset to the end of the page.
			memcpy(
				(uint8_t *)buffer + buffer_offset,
				frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset),
				CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset)
			);
			buffer_offset += CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset);
		}

		// Pages in the middle must be copied in full.
		while (buffer_offset < length - final_partial_length) {

			// Query for the page with the current offset.
			if ((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped, do load op.

				do_load_op(desc_info, curr_frame, curr_page, buffer_offset - desc_info.offset);
			}

			// Here, frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset) gives us the start
			memcpy((uint8_t *)buffer + buffer_offset, frames[curr_frame].memory_region, CS_PAGE_SIZE);
			buffer_offset += CS_PAGE_SIZE;
		}

		// For final potentially partially filled page
		if (final_partial_length) {
			if ((curr_page = get_page_guid(desc_info, desc_info.offset + buffer_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped, do load op.

				do_load_op(desc_info, curr_frame, curr_page, buffer_offset - desc_info.offset);
			}

			memcpy((uint8_t *)buffer + buffer_offset, frames[curr_frame].memory_region, final_partial_length);
			buffer_offset += final_partial_length;
		}

		// We update the current offset at the end of the operation.
		desc_info.offset += buffer_offset;

		// Update descriptor info.
		file_page_map.insert(dd, desc_info);

		return buffer_offset;
	}
}

// Similar to the read operation but opposite data flow.
size_t PageTable::write(data_descriptor dd, void *data, size_t length) {
	Map<data_descriptor, DescriptorInfo>::Element *elem = file_page_map.find(dd);

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		size_t final_partial_length = (length % CS_PAGE_SIZE);
		DescriptorInfo desc_info = elem->get();
		size_t curr_page = CS_MEM_VAL_BAD, curr_frame = CS_MEM_VAL_BAD, data_offset = 0;

		// Special handling of first page.
		{
			if ((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped, do load op.

				do_load_op(desc_info, curr_frame, curr_page, data_offset - desc_info.offset);
			}

			// Set the dirty bit.
			frames.ptrw()[curr_frame].dirty = true;

			memcpy(
				frames[curr_frame].memory_region + PARTIAL_SIZE(desc_info.offset),
				(uint8_t *)data + data_offset,
				CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset)
			);
			// Update offset with number of bytes read in first iteration.
			data_offset += CS_PAGE_SIZE - PARTIAL_SIZE(desc_info.offset);
		}

		while (data_offset < length - final_partial_length) {
			if ((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped, do load op.

				do_load_op(desc_info, curr_frame, curr_page, data_offset - desc_info.offset);
			}

			memcpy(frames[curr_frame].memory_region, (uint8_t *)data + data_offset, CS_PAGE_SIZE);
			data_offset += CS_PAGE_SIZE;
		}

		// For final potentially partially filled page
		if (final_partial_length) {
			if ((curr_page = get_page_guid(desc_info, desc_info.offset + data_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped, do load op.

				do_load_op(desc_info, curr_frame, curr_page, data_offset - desc_info.offset);
			}

			memcpy(frames[curr_frame].memory_region, (uint8_t *)data + data_offset, final_partial_length);
			data_offset += final_partial_length;
		}

		desc_info.offset += data_offset;

		// Update descriptor info.
		file_page_map.insert(dd, desc_info);

		return data_offset;
	}
}


// The seek operation just uses the POSIX seek modes, which will probably get replaced later.
size_t PageTable::seek(data_descriptor dd, size_t new_offset, int mode) {
	Map<data_descriptor, DescriptorInfo>::Element *elem = file_page_map.find(dd);

	if (!elem) {

		return CS_MEM_VAL_BAD;

	} else {

		DescriptorInfo desc_info = elem->get();
		size_t curr_offset = desc_info.offset;
		size_t end_offset = desc_info.total_size;
		size_t eff_offset = 0;
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

		size_t curr_page = CS_MEM_VAL_BAD;
		size_t curr_frame = CS_MEM_VAL_BAD;

		if (eff_offset < 0) {
			ERR_PRINT("Invalid offset.")
			return CS_MEM_VAL_BAD;

		// In this case, there can only be a hole and no data, so we only
		// do a paging operation to represent it in memory, in case of a write.
		} else if (eff_offset > end_offset) {

			if ((curr_page = get_page_guid(desc_info, eff_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped and likely does not contain data, do paging op.

				do_paging_op(desc_info, curr_frame, curr_page, eff_offset - curr_offset);
			}

		} else {
			if ((curr_page = get_page_guid(desc_info, eff_offset, true)) != CS_MEM_VAL_BAD) {
				// Page is mapped, so just get the correct frame.
				curr_frame = page_frame_map[curr_page];

			} else {
				// Page is not mapped but likely to contain data, do load op.

				do_load_op(desc_info, curr_frame, curr_page, eff_offset - curr_offset);
			}
		}

		// Update the offset.
		desc_info.offset = eff_offset;

		// Update descriptor info.
		file_page_map.insert(dd, desc_info);

		return curr_offset;
	}
}
