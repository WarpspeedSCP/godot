#include <assert.h>

#include "core/error_macros.h"
#include "core/engine.h"

#include "file_access_cached.h"

FileAccessCached::FileAccessCached() {
	server = (FileCacheServer *)Engine::get_singleton()->get_singleton_object("FileCacheServer");
	ERR_FAIL_COND(!server);
	sem = Semaphore::create();
	ERR_FAIL_COND(!sem);
}

FileAccessCached::~FileAccessCached() {
	memdelete(sem);
	server->close(cached_file);
}

Error FileAccessCached::_open(const String &p_path, int p_mode_flags) {

	cached_file = server->open(p_path, p_mode_flags);
	if(cached_file.is_valid())
		return OK;
	else
		return ERR_CANT_OPEN;
}

void FileAccessCached::FileAccessCached::close() {
	server->close(cached_file);

} ///< close a file

bool FileAccessCached::is_open() const {
	if(this->cached_file.is_valid()) return true;
	return false;
} ///< true when file is open

void FileAccessCached::seek(size_t p_position) {
	SeekMessage m = SeekMessage(Message::CS_SEEK, cached_file, sem, p_position, SEEK_SET);
	server->push_message((Message *)&m);
	sem->wait();
} ///< seek to a given position

void FileAccessCached::seek_end(int64_t p_position ) {
	SeekMessage m = SeekMessage(Message::CS_SEEK, cached_file, sem, p_position, SEEK_END);
	server->push_message((Message *)&m);
	sem->wait();
} ///< seek from the end of file

size_t FileAccessCached::get_position() const {
	Message m = Message(Message::CS_TELL, cached_file, sem);
	server->push_message(&m);
	sem->wait();
	return m.o_len;
} ///< get position in the file

size_t FileAccessCached::get_len() const {
	Message m = Message(Message::CS_SIZE, cached_file, sem);
	server->push_message(&m);
	sem->wait();
	return m.o_len;
} ///< get size of the file

bool FileAccessCached::eof_reached() const {
	Message m = Message(Message::CS_EOF, cached_file, sem);
	server->push_message(&m);
	sem->wait();
	return (bool)m.o_len;
} ///< reading passed EOF

template <typename T>
_FORCE_INLINE_ T FileAccessCached::get_t() const {
	T buf;
	RWMessage m = RWMessage(Message::CS_READ, cached_file, sem, sizeof(T), &buf);
	server->push_message((Message *)&m);
	sem->wait();
	return buf;
}

uint8_t FileAccessCached::get_8() const {
	return get_t<uint8_t>();
} ///< get a byte

int FileAccessCached::get_buffer(uint8_t *p_dst, int p_length) const {
	RWMessage m = RWMessage(Message::CS_READ, cached_file, sem, p_length, &p_dst);
	server->push_message((Message *)&m);
	sem->wait();
	return m.o_len;
} ///< get an array of bytes

Error FileAccessCached::get_error() const { return last_error; } ///< get last error

void FileAccessCached::flush() {}

template <typename T>
_FORCE_INLINE_ void FileAccessCached::store_t(T buf) {
	RWMessage m = RWMessage(Message::CS_WRITE, cached_file, sem, sizeof(T), &buf);
	server->push_message((Message *)&m);
	sem->wait();
}

void FileAccessCached::store_8(uint8_t p_dest) {
	store_t<uint8_t>(p_dest);
} ///< store a byte

void FileAccessCached::store_buffer(const uint8_t *p_src, int p_length) {
	RWMessage m = RWMessage(Message::CS_WRITE, cached_file, sem, p_length, &p_src);
	server->push_message((Message *)&m);
	sem->wait();
	if(m.i_len < m.o_len)
		ERR_PRINT(("Wrote less than " + itos(m.i_len) + " bytes.\n").utf8().get_data());
} ///< store an array of bytes

bool FileAccessCached::file_exists(const String &p_name) {
	FileAccess *f = FileAccess::create(FileAccess::ACCESS_FILESYSTEM);
	ERR_FAIL_COND_V(!f, false);
	bool exists = f->file_exists(p_name);
	memdelete(f);
	return exists;
} ///< return true if a file exists

Error FileAccessCached::reopen(const String &p_path, int p_mode_flags) { return ERR_PRINTER_ON_FIRE; } ///< does not change the AccessType

