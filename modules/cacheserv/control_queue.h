#ifndef CTRL_QUEUE_H
#define CTRL_QUEUE_H


#include "core/rid.h"
#include "core/list.h"
#include "core/os/mutex.h"
#include "core/os/semaphore.h"

#include "page_table.h"


class CachedResourceHandle : public RID_Data {};

	// union {

	// 	struct {
	// 		size_t in_size;
	// 		size_t *out_size;
	// 	} rw_data;

	// 	struct {
	// 		off64_t seek_offset;
	// 		char seek_arg;
	// 	} seek_data;

	// } extra;

class CtrlOp {
public:

	enum Op {
		LOAD,
		STORE
	};

	DescriptorInfo *di;
	size_t offset;
	uint8_t type;


	CtrlOp() : di(NULL), offset(CS_MEM_VAL_BAD), type(CS_MEM_VAL_BAD) {}
	virtual ~CtrlOp() {}

	CtrlOp(DescriptorInfo *i_di, size_t i_offset, uint8_t i_type) : di(i_di), offset(i_offset), type(i_type) {}
};

class CtrlQueue {

	friend class FileCacheServer;

private:
	List<CtrlOp> queue;
	Mutex *mutex;
	Semaphore *sem;

	CtrlOp pop() {

		if(queue.empty()) {
			sem->wait();
		}

		CtrlOp l = queue.front()->get();
		queue.pop_front();

		return l;
	}

public:

	CtrlQueue() {
		mutex = Mutex::create();
		sem = Semaphore::create();
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