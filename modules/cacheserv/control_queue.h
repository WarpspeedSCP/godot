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
		QUIT,
		FLUSH,
		FLUSH_CLOSE,
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
	Mutex *mut;
	Semaphore *sem;

	CtrlOp pop() {
		while(queue.empty()) {
			sem->wait();
		}

		if(sig_quit) return CtrlOp();

		// We only need to lock when accessing the queue.
		MutexLock ml(mut);

		CtrlOp op = queue.front()->get();
		queue.pop_front();

		return op;
	}

public:

	bool sig_quit;

	CtrlQueue() {
		mut = Mutex::create();
		sem = Semaphore::create();
		sig_quit = false;
	}

	~CtrlQueue() {
		memdelete(mut);
		memdelete(sem);
	}

	void push(CtrlOp op) {
		MutexLock ml = MutexLock(mut);
			queue.push_back(op);
			sem->post();
		WARN_PRINTS("Pushed op")
	}

	/**
	 * Pushes to the queue's front, so the pushed operation is processed ASAP.
	 */
	void priority_push(CtrlOp op) {
		MutexLock ml = MutexLock(mut);
			queue.push_front(op);
			sem->post();
		WARN_PRINTS("Priority pushed op.")
	}

};

#endif //CTRL_QUEUE_H
