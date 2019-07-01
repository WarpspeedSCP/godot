#ifndef CTRL_QUEUE_H
#define CTRL_QUEUE_H


#include "core/rid.h"
#include "core/list.h"
#include "core/os/mutex.h"
#include "core/os/semaphore.h"

#include "cache_info_table.h"


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


	CtrlOp() : di(NULL), offset(CS_MEM_VAL_BAD), type(QUIT) {}

	CtrlOp(DescriptorInfo *i_di, frame_id frame, size_t i_offset, uint8_t i_type) : di(i_di), frame(frame), offset(i_offset), type(i_type) {}
};

class CtrlQueue {

	friend class FileCacheManager;

private:
	List<CtrlOp> queue;
	Mutex *mutex;
	Semaphore *sem;

	CtrlOp pop() {

		while(queue.empty()) {
			sem->wait();
		}

		if(sig_quit) return CtrlOp();

		CtrlOp l = queue.front()->get();
		queue.pop_front();

		return l;
	}

	CtrlOp try_pop() {
		if(queue.empty()) {
			return CtrlOp();
		}

		CtrlOp l = queue.front()->get();
		queue.pop_front();

		return l;
	}

public:

	bool sig_quit;

	CtrlQueue() {
		mutex = Mutex::create();
		sem = Semaphore::create();
		sig_quit = false;
	}

	~CtrlQueue() {
		memdelete(mutex);
		memdelete(sem);
	}

	void push(CtrlOp l) {
		MutexLock ml = MutexLock(mutex);
			queue.push_back(l);
			sem->post();
	}

};

#endif //CTRL_QUEUE_H