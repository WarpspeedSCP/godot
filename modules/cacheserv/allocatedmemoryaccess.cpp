#include "allocatedmemoryaccess.h"
#define REM_LEN_IN_CURR_REGION curr_region.size *CS_PAGE_SIZE - offset_in_curr_region

AllocatedMemoryAccess::AllocatedMemoryAccess(size_t length, PageTable *i_pt) {
	pt = i_pt;
	id = pt->allocate(length);
	total_len = length;
	alloc_regions = pt->list_regions(id);
	total_regions = alloc_regions.size();
}

// Write data with length length to the memory. Returns -1 if length is greater than the allocated length. Returns excess length if the
size_t AllocatedMemoryAccess::write(uint8_t *data, size_t length) {

	Region curr_region = alloc_regions[offset_region];
	size_t data_offset = 0;
	size_t rem_len = length;

	if (total_len < length)
		return -1;

	while (rem_len && offset_region < total_regions) {
		if (rem_len > REM_LEN_IN_CURR_REGION) {
			memcpy(curr_region.mem_ptr + offset_in_curr_region, data + data_offset, REM_LEN_IN_CURR_REGION);
			data_offset += REM_LEN_IN_CURR_REGION;
			rem_len -= REM_LEN_IN_CURR_REGION;
			offset_in_curr_region = 0;
			offset_region++;
			curr_region = alloc_regions[offset_region];
		} else {

			memcpy(curr_region.mem_ptr + offset_in_curr_region, data + data_offset, rem_len);
			offset_in_curr_region += rem_len;
			rem_len = 0;
		}
	}

	if (rem_len)
		return length - rem_len;

	return 0;
}

size_t AllocatedMemoryAccess::read(uint8_t *buf, size_t length) {

	Region curr_region = alloc_regions[offset_region];
	size_t data_offset = 0;
	size_t rem_len = length;

	if (total_len < length)
		return -1;

	while (rem_len && offset_region < total_regions) {

		pt->set_dirty(curr_region.start_page_idx);
		if (rem_len > REM_LEN_IN_CURR_REGION) {

			memcpy(buf + data_offset, curr_region.mem_ptr + offset_in_curr_region, REM_LEN_IN_CURR_REGION);
			data_offset += REM_LEN_IN_CURR_REGION;
			rem_len -= REM_LEN_IN_CURR_REGION;
			offset_in_curr_region = 0;
			offset_region++;
			curr_region = alloc_regions[offset_region];
		} else {

			memcpy(buf + data_offset, curr_region.mem_ptr + offset_in_curr_region, rem_len);
			offset_in_curr_region += rem_len;
			rem_len = 0;
		}
	}

	if (length > data_offset)
		return -1;

	return 0;
}

size_t AllocatedMemoryAccess::seek(size_t off, int mode) {

	switch (mode) {
		case SEEK_SET:
			if (off < 0)
				return -1;
			offset = off;
			offset_region = off / CS_PAGE_SIZE;
			offset_in_curr_region = off % CS_PAGE_SIZE;
			return off;
		case SEEK_CUR:
			if (offset + off > total_len || offset + off < 0)
				return -1;
			offset += off;
			offset_region = offset / CS_PAGE_SIZE;
			offset_in_curr_region = offset % CS_PAGE_SIZE;
			return offset;
        case SEEK_END:
	}
	return 0;
}

#undef REM_LEN_IN_CURR_REGION