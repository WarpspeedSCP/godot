#include "pagetable.h"

PageTable::~PageTable() {
	if (memory_region) {
		memdelete_arr(memory_region);
	}
}

PageTable::PageTable() {

	page_frame_map.clear();
	file_page_map.clear();
	frames.clear();

	available_space = CS_CACHE_SIZE;
	used_space = 0;
	total_space = CS_CACHE_SIZE;

	if (memory_region)
		memdelete_arr(memory_region);
	memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);

	for (size_t i = 0; i < CS_NUM_FRAMES; ++i)
		frames.push_back(
			Frame(
				(memory_region + i * CS_PAGE_SIZE),
				CS_MEM_VAL_BAD
			)
		);

}


/*

{
	int fd = open(fiel);
	read(fd, buf, n_bytes);
		|
		V
		Check if present in cache ... Any pages corresponding to fd in file_page_map?
			y: Do-
				1. Is page for this offset mapped ... Page with offset exists in file_page_map.value?
					y: Return frame mapped to page ... Return page_frame_map.value_of(page).
					n: Cache miss.
						1. Perform load op.
							Load op:
							1. Add a page to file_page_map.value
							2. Get an empty frame ... Perform paging op, get frame id for mapping.
								Paging op:
								1. Is the cache full?
									y: Select old frame to evict ... Perform according to paging/caching algo.
										...
									n: Do-
										1. Select an empty frame.
										2. Mark frame as used.
								Return selected frame.
							3. Map page to frame.
							Return page id for newly mapped page.
						2. Get frame mapped to loaded page.
						3. Copy data at offset from data source to frame.
						Return frame.
				2. Copy from mapped frame to buffer.
				3. Repeat steps 1 & 2 until all data paged & read to buffer.
				Return success.
			n: Do-
				1. Add a page to the page vec for fd.
				2. Do same op as y branch.

}

*/