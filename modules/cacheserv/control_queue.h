#ifndef CTRL_QUEUE_H
#define CTRL_QUEUE_H


#include "core/rid.h"
#include "core/list.h"
#include "core/os/mutex.h"
#include "core/os/semaphore.h"

#include "data_helpers.h"


class CachedResourceHandle : public RID_Data {};

struct CtrlOp {
	enum Op {
		LOAD,
		STORE,
		QUIT
	};

	DescriptorInfo *di;
	frame_id frame;
	size_t offset;
	uint8_t type;


	CtrlOp() : di(NULL), frame(CS_MEM_VAL_BAD), offset(CS_MEM_VAL_BAD), type(QUIT) {}

	CtrlOp(DescriptorInfo *i_di, frame_id frame, size_t i_offset, uint8_t i_type) : di(i_di), frame(frame), offset(i_offset), type(i_type) {}
};

class CtrlQueue {

	friend class FileCacheManager;

private:
	List<CtrlOp> queue;
	Mutex *client_mut;
	Mutex *server_mut;
	Semaphore *sem;

	CtrlOp pop() {
		MutexLock ml(server_mut);
		while(queue.empty()) {
			sem->wait();
		}

		if(sig_quit) return CtrlOp();

		CtrlOp l = queue.front()->get();
		queue.pop_front();

		return l;
	}

	void lock() { server_mut->lock(); }
	void unlock() { server_mut->unlock(); }

	// CtrlOp try_pop() {
	// 	if(queue.empty()) {
	// 		return CtrlOp();
	// 	}

	// 	CtrlOp l = queue.front()->get();
	// 	queue.pop_front();

	// 	return l;
	// }

public:

	bool sig_quit;

	CtrlQueue() {
		client_mut = Mutex::create();
		server_mut = Mutex::create();
		sem = Semaphore::create();
		sig_quit = false;
	}

	~CtrlQueue() {
		memdelete(client_mut);
		memdelete(server_mut);
		memdelete(sem);
	}

	void push(CtrlOp l) {
		MutexLock ml = MutexLock(client_mut);
			queue.push_back(l);
			sem->post();
	}

};

#endif //CTRL_QUEUE_H