#ifndef CS_MESSAGE_QUEUE_H
#define CS_MESSAGE_QUEUE_H


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

class Message {
public:
	enum MessageType {
		CS_READ,
		CS_WRITE,
		CS_SEEK,
		CS_TELL,
		CS_SIZE,
		CS_EOF
	};
	char type;
	RID hndl;
	size_t i_len;
	size_t o_len;
	// MUST POINT TO VOLATILE. maybe not.
	Semaphore *done;

	virtual ~Message() {}

	Message(char type, RID hndl, Semaphore *sem, size_t i_len = 0) : type(type), hndl(hndl), done(sem), i_len(i_len) {}
};

class RWMessage : public Message {
public:
	void *data;

	RWMessage(
		char type,
		RID hndl,
		Semaphore *sem,
		size_t i_len,
		void *data
	) :
		Message(type, hndl, sem, i_len),
		data(data)
	{}
};

class SeekMessage : public Message {
public:
	char arg;


	SeekMessage(
		char type,
		RID hndl,
		Semaphore *sem,
		size_t i_len = 0,
		char arg = -1
	) :
		Message(type, hndl, sem, i_len),
		arg(arg)
	{}
};

class MQueue {

	friend class FileCacheServer;

private:
	List<Message *> mqueue;
	Mutex *mutex;
	Semaphore *sem;

	Message *pop() {

		if(mqueue.empty()) {
			sem->wait();
		}

		Message *m = mqueue.front()->get();
		mqueue.pop_front();

		return m;
	}

public:

	MQueue() {
		mutex = Mutex::create();
		sem = Semaphore::create();
	}

	void push(Message *m) {
		MutexLock ml = MutexLock(mutex);
			mqueue.push_back(m);
			sem->post();
	}

};

#endif //CS_MESSAGE_QUEUE_H