/*************************************************************************/
/*  file_cache_server.h                                                  */
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

#ifndef FILE_CACHE_SERVER_H
#define FILE_CACHE_SERVER_H

#include "core/object.h"
#include "core/os/mutex.h"
#include "core/os/thread.h"
#include "core/rid.h"
#include "core/set.h"
#include "core/variant.h"
#include "core/vector.h"
#include "core/error_macros.h"


#include "cacheserv_defines.h"
#include "page_table.h"
#include "message_queue.h"


class FileCacheServer : public Object {
	GDCLASS(FileCacheServer, Object);

	static FileCacheServer *singleton;
	bool exit_thread;
	PageTable page_table;
	RID_Owner<CachedResourceHandle> handle_owner;
	HashMap<uint32_t, data_descriptor> files;
	MQueue mqueue;
	Thread *thread;
	Thread *io_thread;
	Mutex *mutex;

private:
	static void thread_func(void *p_udata);

protected:

public:
	FileCacheServer();
	~FileCacheServer();

	Variant _get_state() {

		List<uint32_t> keys;
		files.get_key_list(&keys);
		Dictionary d;
		for(List<uint32_t>::Element *i = keys.front(); i; i = i->next()) {
			d[page_table.file_page_map[i->get()]->internal_data_source->get_path()] = page_table.file_page_map[i->get()]->to_variant(page_table);
		}

		return Variant(d);
	}

	static FileCacheServer *get_singleton();

	Error init();

	// Returns an RID to an opened file.
	RID open(String path, int p_mode) {
		printf("%s %d\n", path.utf8().get_data(), p_mode);
		ERR_FAIL_COND_V(!mutex, RID());
		MutexLock ml = MutexLock(mutex);
			ERR_FAIL_COND_V(path.empty(), RID());

			// Will be freed when close is called with the corresponding RID.
			CachedResourceHandle *hdl = memnew(CachedResourceHandle);
			RID rid = handle_owner.make_rid(hdl);
			ERR_FAIL_COND_V(!rid.is_valid(), RID());
			FileAccess *fa = FileAccess::open(path, p_mode);
			files[rid.get_id()] = page_table.add_data_source(fa); // TODO: Change to actual file creation code.
			printf("open file %s with mode %d\nGot RID %d\n", path.utf8().get_data(), p_mode, rid.get_id());
			return rid;
	}

	// Invalidates the pointer. The pointer *will not* be valid after a call to this function.
	void close(RID rid) {
		printf("close file with RID %d\n", rid.get_id());
		MutexLock ml = MutexLock(mutex);
			handle_owner.free(rid);
			memfree(rid.get_data());


	}

	void push_message(Message *m) {
		mqueue.push(m);
	}

	void lock();
	void unlock();



};

class _FileCacheServer : public Object {
	GDCLASS(_FileCacheServer, Object);

	friend class FileCacheServer;
	static _FileCacheServer *singleton;

protected:
	static void _bind_methods() {
		ClassDB::bind_method(D_METHOD("get_state"), &_FileCacheServer::get_state);
	}

public:
	_FileCacheServer();
	static _FileCacheServer *get_singleton();
	Variant get_state() { return FileCacheServer::get_singleton()->_get_state();}


};

#endif // FILE_CACHE_SERVER_H
