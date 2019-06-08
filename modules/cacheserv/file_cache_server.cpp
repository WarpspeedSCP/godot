#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "core/io/file_access_memory.h"

#include "file_cache_server.h"


FileCacheServer::FileCacheServer() {
	singleton = this;
}

FileCacheServer::~FileCacheServer() {}

FileCacheServer *FileCacheServer::singleton = NULL;
_FileCacheServer *_FileCacheServer::singleton = NULL;

FileCacheServer *FileCacheServer::get_singleton() {
	return singleton;
}

void FileCacheServer::_bind_methods() {}
void _FileCacheServer::_bind_methods() {}

void FileCacheServer::unlock() {
	if (!thread || !mutex) {
		return;
	}

	mutex->unlock();
}

void FileCacheServer::lock() {
	if (!thread || !mutex) {
		return;
	}

	mutex->lock();
}



Error FileCacheServer::init() {
	exit_thread = false;
	mutex = Mutex::create();
	thread = Thread::create(FileCacheServer::thread_func, this);
	return OK;
}

void FileCacheServer::thread_func(void *p_udata) {
	FileCacheServer &fcs = *(FileCacheServer *)p_udata;

	// while (!(fcs->exit_thread)) {
	// 	sleep(10);
	// }
	PageTable &p = fcs.page_table;
	FileAccessMemory f;


	uint8_t bytes[1024], tytes[1024];

	{
		FILE *f = fopen("modules/cacheserv/file_cache_server.cpp", "r");
		fread(bytes, 1024, 1000, f);
		fclose(f);
	}


	f.open_custom(bytes, 1024);
	int dd = p.add_data_source((FileAccess *) &f);

	p.read(dd, tytes, 30);

	p.seek(dd, 800, SEEK_SET);

	p.read(dd, tytes + 30, 60);


	f.close();
	print_line("It woerks");
}

_FileCacheServer::_FileCacheServer() {
	singleton = this;
}

_FileCacheServer *_FileCacheServer::get_singleton() {
	return singleton;
}
