#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "drivers/unix/file_access_unix.h"


#include "file_cache_server.h"
#include "file_access_cached.h"


FileCacheServer::FileCacheServer() {
	singleton = this;
}

FileCacheServer::~FileCacheServer() {}

FileCacheServer *FileCacheServer::singleton = NULL;
_FileCacheServer *_FileCacheServer::singleton = NULL;

FileCacheServer *FileCacheServer::get_singleton() {
	return singleton;
}

// void FileCacheServer::_bind_methods() {
// 	ClassDB::bind_method(D_METHOD("_get_state"), &FileCacheServer::_get_state);
// }
// void _FileCacheServer::_bind_methods() {}

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

	sleep(2);

	RID x = open("/home/warpspeedscp/godot/big.txt", FileAccess::READ);
	Semaphore *s1 = Semaphore::create();
	SeekMessage m = SeekMessage(Message::CS_SIZE, x, s1);
	this->push_message(&m);
	s1->wait();
	printf("size is : %lx\n", m.o_len);
	memdelete(s1);
	close(x);

	return OK;
}

#define DBG_PRINT //	printf("\n\n");\
// 	for(auto i = p.page_frame_map.front(); i; i = i->next()) \
// 		printf("%lx : %lx\n", i->key(), i->value()); \
// 	printf("\n\n");


void FileCacheServer::thread_func(void *p_udata) {
	srandom(time(0));
	FileCacheServer &fcs = *(FileCacheServer *)p_udata;


	// FileAccess::make_default<FileAccessUnbuffered>(FileAccess::ACCESS_FILESYSTEM);

	// while (!(fcs->exit_thread)) {
	// 	sleep(10);
	// }

	// FileAccess * r = memnew(FileAccessCached);

	// PageTable &p = fcs.page_table;
	// FileAccessUnix *f, *g;
	// f = memnew(FileAccessUnix);
	// g = f;
	// f = (FileAccessUnix *)f->open(String("nbig.txt"), FileAccess::READ_WRITE);
	// memdelete(g);

	// uint8_t bytes[CS_PAGE_SIZE * 16], tytes[CS_PAGE_SIZE * 16], fytes[CS_PAGE_SIZE * 16], qites[CS_PAGE_SIZE * 16];

	// DBG_PRINT

	// int dd = p.add_data_source((FileAccess *) f);

	// // DBG_PRINT

	// // int ee = p.add_data_source((FileAccess *) &g);

	// DBG_PRINT

	// p.read(dd, tytes, 6 * CS_PAGE_SIZE);

	// DBG_PRINT

	// // p.seek(ee, 21 * CS_PAGE_SIZE, SEEK_SET);

	// // DBG_PRINT

	// // p.read(ee, qites, 3 * CS_PAGE_SIZE);

	// // DBG_PRINT

	// p.seek(dd, 8 * CS_PAGE_SIZE, SEEK_SET);

	// DBG_PRINT

	// // p.seek(ee, 0, SEEK_SET);

	// // DBG_PRINT

	// p.write(dd, tytes, 200);

	// DBG_PRINT

	// p.read(dd, tytes, 5 * CS_PAGE_SIZE);

	// DBG_PRINT

	// f->close();
	// memdelete(f);
	// print_line("It woerks");

	while(!fcs.exit_thread) {

		Message *m = fcs.mqueue.pop();
		printf("got message\n");
		ERR_CONTINUE(m == NULL);
		ERR_CONTINUE(m->done == NULL);
		ERR_CONTINUE(!m->hndl.is_valid());


		data_descriptor dd = fcs.files[m->hndl.get_id()];
		printf("message RID: %d\n", m->hndl.get_id());
		switch(m->type) {

			case Message::CS_READ: {
				RWMessage *rwm = dynamic_cast<RWMessage *>(m);
				ERR_CONTINUE(rwm == NULL)
				ERR_CONTINUE(rwm->data == NULL)
				rwm->o_len = fcs.page_table.read(dd, rwm->data, rwm->i_len);
				break;
			}
			case Message::CS_WRITE: {
				RWMessage *rwm = dynamic_cast<RWMessage *>(m);
				ERR_CONTINUE(rwm == NULL)
				ERR_CONTINUE(rwm->data == NULL)
				rwm->o_len = fcs.page_table.write(dd, rwm->data, rwm->i_len);
				break;
			}
			case Message::CS_SEEK: {
				SeekMessage *sm = dynamic_cast<SeekMessage *>(m);
				ERR_CONTINUE(sm == NULL);
				// Update data pointer with new offset.
				sm->o_len = fcs.page_table.seek(dd, sm->i_len, sm->arg);
				printf("Sent length of %lx\n", sm->o_len);
				break;
			}
			case Message::CS_TELL: {
				//Return value through data pointer.
				m->o_len = fcs.page_table.seek(dd, 0, SEEK_CUR);
				break;
			}
			case Message::CS_SIZE: {
				m->o_len = fcs.page_table.get_len(dd);
				printf("Sent length of %lx\n", m->o_len);
				break;
			}
			case Message::CS_EOF: {
				m->o_len = fcs.page_table.eof_reached(dd);

				break;
			}
			default:
				ERR_PRINT("Invalid message received. Ignoring message.\n");
		}

		m->done->post();
		printf("Posted sem\n");
	}
}

_FileCacheServer::_FileCacheServer() {
	singleton = this;
}

_FileCacheServer *_FileCacheServer::get_singleton() {
	return singleton;
}
