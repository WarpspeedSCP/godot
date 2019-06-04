#include "pagetable.h"

PageTable::~PageTable() {
	if (this->memory_region) {
		memdelete_arr(this->memory_region);
	}
}

void PageTable::create() {
	this->free_regions.clear();
	this->used_regions.clear();
	this->pages.clear();
	this->available_space = CS_CACHE_SIZE;
	this->used_space = 0;
	this->total_space = CS_CACHE_SIZE;

	if (this->memory_region)
		memdelete_arr(this->memory_region);
	this->memory_region = memnew_arr(uint8_t, CS_CACHE_SIZE);

	for (size_t i = 0; i < CS_NUM_PAGES; ++i) {
		this->pages.push_back(
				Page(
						(this->memory_region + i * CS_PAGE_SIZE),
						CS_MEM_VAL_BAD));
	}

	this->free_regions.erase((size_t)0);
	this->free_regions.insert(0, Region(0, CS_NUM_PAGES, CS_MEM_VAL_BAD, CS_MEM_VAL_BAD));
}

// Allocate length bytes of memory in the cache.
size_t PageTable::allocate(size_t length) {
	size_t curr_start_idx = 0;
	size_t start_idx = 0;
	size_t data_offset = 0;
	size_t paged_length = length / CS_PAGE_SIZE + (length % CS_PAGE_SIZE == 0 ? 0 : 1);

	if (this->pages.size() < (int)paged_length)
		return CS_MEM_VAL_BAD;

	if (this->free_regions.size() == 1 && this->used_regions.size() == 0) {

		this->free_regions.erase((size_t)0);

		prepare_region(curr_start_idx, paged_length, &data_offset);

		this->used_regions.insert(0, Region(curr_start_idx, paged_length, CS_MEM_VAL_BAD, CS_MEM_VAL_BAD));
		this->free_regions.insert(curr_start_idx + paged_length, Region(curr_start_idx + paged_length, CS_NUM_PAGES - paged_length, CS_MEM_VAL_BAD, CS_MEM_VAL_BAD));

		//this->last_alloc_end = curr_start_idx + paged_length;

	} else {

		auto next_free_region = this->free_regions.front();
		size_t curr_available_size = next_free_region->get().size;
		size_t prev_region = CS_MEM_VAL_BAD;
		size_t rem_length = paged_length;

		start_idx = curr_start_idx = next_free_region->get().start_page_idx;

		while (true) {
			if (this->free_regions.size() == 0) {
				create();
				return allocate(length);
			}

			if (rem_length > curr_available_size) {
				prepare_region(curr_start_idx, curr_available_size, &data_offset);

				if (prev_region != CS_MEM_VAL_BAD)
					this->used_regions[prev_region].next = curr_start_idx;

				rem_length -= curr_available_size;

				// Erase currently used free region and unlink it from the previous one.
				size_t prev_free = this->free_regions[curr_start_idx].prev;
				if (prev_free != CS_MEM_VAL_BAD)
					this->free_regions[prev_free].next = this->free_regions[curr_start_idx].next;
				this->free_regions.erase(curr_start_idx);

				// Add a new entry to used_regions.
				this->used_regions.insert(curr_start_idx, Region(curr_start_idx, curr_available_size, prev_region, CS_MEM_VAL_BAD));
				prev_region = curr_start_idx;
				next_free_region = this->free_regions.front();
				curr_available_size = next_free_region->get().size;
				curr_start_idx = next_free_region->get().start_page_idx;
			} else {
				prepare_region(curr_start_idx, rem_length, &data_offset);

				size_t prev_free = this->free_regions[curr_start_idx].prev;
				if (prev_free != CS_MEM_VAL_BAD)
					this->free_regions[prev_free].next = CS_MEM_VAL_BAD;

				if (prev_region != CS_MEM_VAL_BAD)
					this->used_regions[prev_region].next = curr_start_idx;

				this->used_regions.insert(curr_start_idx, Region(curr_start_idx, rem_length, prev_region, CS_MEM_VAL_BAD));

				// Insert a new free region in the place of the old one.
				if (curr_available_size - rem_length > 0)
					this->free_regions.insert(curr_start_idx + rem_length, Region(curr_start_idx + rem_length, curr_available_size - rem_length, prev_free, this->free_regions[curr_start_idx].next));
				this->free_regions.erase(curr_start_idx);
				break;
			}
		}
	}

	return start_idx;
}

// Free a set of regions.
void PageTable::free(size_t index) {

	Region curr_used_region = Region();
	while (index != CS_MEM_VAL_BAD) {

		curr_used_region = this->used_regions[index];
		for (size_t i = curr_used_region.start_page_idx; i < curr_used_region.size + curr_used_region.start_page_idx; ++i) {

			memset((void *)(this->pages[i].memory_region), 0, CS_PAGE_SIZE);
			this->pages.ptrw()[i].used = false;
			this->pages.ptrw()[i].data_offset = CS_MEM_VAL_BAD;
		}

		bool can_merge_prev = false, can_merge_next = false, can_merge = false;

		if (this->free_regions.size() == 1) {

			auto free_region = this->free_regions.front()->get();

			// Check if the free region is before or after the current one.
			if (free_region.start_page_idx != CS_MEM_VAL_BAD && free_region.start_page_idx + free_region.size == curr_used_region.start_page_idx) {
				can_merge = true;
				free_region.size += curr_used_region.size;
				this->free_regions.insert(free_region.start_page_idx, free_region);

			} else if (curr_used_region.start_page_idx + curr_used_region.size == free_region.start_page_idx) {
				can_merge = true;
				curr_used_region.size += free_region.size;
				this->free_regions.insert(curr_used_region.start_page_idx, curr_used_region);
				this->free_regions.erase(free_region.start_page_idx);
			}

		} else if (this->free_regions.size() > 0) {

			for (auto i = this->free_regions.front(); i; i = i->next()) {

				Region next = i->get();

				if (next.start_page_idx > index) {

					Region prev = Region();
					Region nnext = Region();

					if (next.prev != CS_MEM_VAL_BAD)
						prev = this->free_regions[next.prev];

					// Check if we can merge the current region with the previous one.
					if (prev.start_page_idx != CS_MEM_VAL_BAD && prev.start_page_idx + prev.size == curr_used_region.start_page_idx)
						can_merge_prev = true, can_merge = true;

					// Check if we can merge the next region with the current one.
					if (curr_used_region.start_page_idx + curr_used_region.size == next.start_page_idx) {
						can_merge_next = true;
						can_merge = true;
						nnext = this->free_regions[next.next];
					}

					// If we can merge into the previous region...
					if (can_merge_prev)
						prev.size += curr_used_region.size;

					if (can_merge_next)
						if (can_merge_prev)
							prev.size += next.size;
						else
							curr_used_region.size += next.size;

					// Update links...
					if (can_merge_prev) {
						if (can_merge_next) {
							prev.next = next.next;
							nnext.prev = prev.start_page_idx;
						} else {
							prev.next = next.start_page_idx;
							next.prev = prev.start_page_idx;
						}
					} else {
						if (can_merge_next) {
							prev.next = curr_used_region.start_page_idx;
							curr_used_region.prev = prev.start_page_idx;
							curr_used_region.next = next.next;
							nnext.prev = curr_used_region.start_page_idx;
						} else {
							prev.next = curr_used_region.start_page_idx;
							curr_used_region.prev = prev.start_page_idx;
							curr_used_region.next = next.start_page_idx;
							next.prev = curr_used_region.start_page_idx;
						}
					}

					this->free_regions.insert(prev.start_page_idx, prev);

					if (can_merge_prev) {
						if (can_merge_next) {
							this->free_regions.erase(next.start_page_idx);

							this->free_regions.insert(nnext.start_page_idx, nnext);
						} else {
							this->free_regions.insert(next.start_page_idx, next);
						}
					} else {
						if (can_merge_next) {
							this->free_regions.insert(curr_used_region.start_page_idx, curr_used_region);

							this->free_regions.erase(next.start_page_idx);

							this->free_regions.insert(nnext.start_page_idx, nnext);
						} else {
							this->free_regions.insert(curr_used_region.start_page_idx, curr_used_region);

							this->free_regions.insert(next.start_page_idx, next);
						}
					}

					break;
				}

				if (!i->next()) {

					Region prev = i->get();

					if (prev.start_page_idx != CS_MEM_VAL_BAD && prev.start_page_idx + prev.size == curr_used_region.start_page_idx)
						can_merge_prev = true, can_merge = true;

					if (can_merge_prev)
						prev.size += curr_used_region.size;
					else {

						prev.next = curr_used_region.start_page_idx;
						curr_used_region.prev = prev.start_page_idx;

						// Since this will only occur if we can't merge with previous, this is safe.
						this->free_regions.insert(curr_used_region.start_page_idx, curr_used_region);
					}

					// We will always execute this.
					this->free_regions.insert(prev.start_page_idx, prev);

					break;
				}
			}
		}

		if (!can_merge) {
			this->free_regions.insert(curr_used_region.start_page_idx, curr_used_region);
		}

		this->used_regions.erase(index);

		index = curr_used_region.next;
	}
}

inline void PageTable::prepare_region(size_t start, size_t size, size_t *data_offset) {
	for (size_t i = start; i < start + size; ++i) {
		this->pages.ptrw()[i].used = true;
		this->pages.ptrw()[i].data_offset = *data_offset;
		*data_offset += CS_PAGE_SIZE;
	}
}
