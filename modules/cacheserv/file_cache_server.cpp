#include <unistd.h>
#include <fcntl.h>
#include <time.h>


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

#define DBG_PRINT //	printf("\n\n");\
// 	for(auto i = p.page_frame_map.front(); i; i = i->next()) \
// 		printf("%lx : %lx\n", i->key(), i->value()); \
// 	printf("\n\n");


void FileCacheServer::thread_func(void *p_udata) {
	srandom(time(0));
	FileCacheServer &fcs = *(FileCacheServer *)p_udata;

	// while (!(fcs->exit_thread)) {
	// 	sleep(10);
	// }
	PageTable &p = fcs.page_table;
	FileAccessMemory f, g;


	uint8_t bytes[CS_PAGE_SIZE * 16], tytes[CS_PAGE_SIZE * 16], fytes[CS_PAGE_SIZE * 16], qites[CS_PAGE_SIZE * 16];
	Vector<uint8_t> btyes;


	int f1 = open("big.txt", O_RDONLY);
	off_t curr_pos = lseek(f1, 0, SEEK_SET);
	while(read(f1, bytes, CS_PAGE_SIZE * 8) > 0){
		curr_pos = lseek(f1, curr_pos + CS_PAGE_SIZE * 8, SEEK_SET);

		for(int i = 0; i < CS_PAGE_SIZE * 8; ++i)
			btyes.push_back(bytes[i]);
	}

	close(f1);

	f.open_custom(btyes.ptr(), btyes.size() / 2);
	g.open_custom(btyes.ptr() + btyes.size() / 2, btyes.size() / 2 - 1);

	DBG_PRINT

	int dd = p.add_data_source((FileAccess *) &f);

	DBG_PRINT

	int ee = p.add_data_source((FileAccess *) &g);

	DBG_PRINT

	p.read(dd, tytes, 6 * CS_PAGE_SIZE);

	DBG_PRINT

	p.seek(ee, 21 * CS_PAGE_SIZE, SEEK_SET);

	DBG_PRINT

	p.read(ee, qites, 3 * CS_PAGE_SIZE);

	DBG_PRINT

	p.seek(dd, 62 * CS_PAGE_SIZE, SEEK_SET);

	DBG_PRINT

	p.seek(ee, 0, SEEK_SET);

	DBG_PRINT

	p.write(ee, tytes, 200);

	DBG_PRINT

	p.read(dd, tytes, 5 * CS_PAGE_SIZE);

	DBG_PRINT

	f.close();
	print_line("It woerks");
}

_FileCacheServer::_FileCacheServer() {
	singleton = this;
}

_FileCacheServer *_FileCacheServer::get_singleton() {
	return singleton;
}
