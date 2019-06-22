/*************************************************************************/
/*  file_access_cached.h                                                 */
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

#ifndef FILE_ACCESS_CACHED_H
#define FILE_ACCESS_CACHED_H

#include "core/engine.h"
#include "core/object.h"
#include "core/os/file_access.h"
#include "core/os/semaphore.h"

#include "file_cache_manager.h"

class _FileAccessCached;

class FileAccessCached : public FileAccess, public Object {
	GDCLASS(FileAccessCached, Object);

	friend class _FileAccessCached;
	friend class FileCacheManager;

	static FileAccess *create() { return (FileAccess *)memnew(FileAccessCached); }

private:
    String rel_path;
    String abs_path;

	Error last_error;

	FileCacheManager *cache_mgr;
	RID cached_file;
	Semaphore *sem;

protected:

	virtual Error _open(const String &p_path, int p_mode_flags) {

		cached_file = cache_mgr->open(p_path, p_mode_flags);
		if(cached_file.is_valid())
			return OK;
		else
			return ERR_CANT_OPEN;
	} ///< open a file

	template <typename T>
	_FORCE_INLINE_ T get_t() const {

		T buf = CS_MEM_VAL_BAD;
		cache_mgr->check_cache(&cached_file, sizeof(T));
		int o_length = cache_mgr->read(&cached_file, &buf, sizeof(T));
		ERR_FAIL_COND_V(buf == (T)CS_MEM_VAL_BAD, CS_MEM_VAL_BAD);
		return buf;
	}

	template <typename T>
	void store_t(T buf) {

		cache_mgr->check_cache(&cached_file, sizeof(T));
		int o_length = cache_mgr->write(&cached_file, &buf, sizeof(T));
	}

public:

	virtual void close() { cache_mgr->close(cached_file); } ///< close a file

	virtual bool is_open() const { return this->cached_file.is_valid(); } ///< true when file is open

	virtual String get_path() const { return rel_path; } /// returns the path for the current open file
	virtual String get_path_absolute() const { return abs_path; } /// returns the absolute path for the current open file

	virtual void seek(size_t p_position) { cache_mgr->seek(&cached_file, p_position); } ///< seek to a given position

	virtual void seek_end(int64_t p_position ) { cache_mgr->seek_end(&cached_file, p_position); } ///< seek from the end of file

	virtual size_t get_position() const { return cache_mgr->get_len(&cached_file); } ///< get position in the file

	virtual size_t get_len() const { return cache_mgr->get_len(&cached_file); } ///< get size of the file

	virtual bool eof_reached() const { return cache_mgr->eof_reached(&cached_file); } ///< reading passed EOF

	virtual uint8_t get_8() const { return get_t<uint8_t>(); } ///< get a byte

	virtual int get_buffer(uint8_t *p_dst, int p_length) const {
		cache_mgr->check_cache(&cached_file, p_length);
		int o_length = cache_mgr->read(&cached_file, p_dst, p_length);
		return o_length;
	} ///< get an array of bytes

	virtual Error get_error() const { return last_error; } ///< get last error

	virtual void flush() { cache_mgr->flush(&cached_file); }

	virtual void store_8(uint8_t p_dest) { return store_t(p_dest); }  ///< store a byte

	virtual void store_buffer(const uint8_t *p_src, int p_length) {

		cache_mgr->check_cache(&cached_file, p_length);
		int o_length = cache_mgr->write(&cached_file, p_src, p_length);
		if(p_length < o_length)
			ERR_PRINT(("Wrote less than " + itos(p_length) + " bytes.\n").utf8().get_data());
	} ///< store an array of bytes

	virtual bool file_exists(const String &p_name) { return cache_mgr->file_exists(p_name); } ///< return true if a file exists

	virtual uint64_t _get_modified_time(const String &p_file) { return 0; }

	virtual Error _chmod(const String &p_path, int p_mod) { return ERR_UNAVAILABLE; }

	virtual Error reopen(const String &p_path, int p_mode_flags) {return ERR_UNAVAILABLE; } ///< does not change the AccessType

	FileAccessCached() {

		cache_mgr = static_cast<_FileCacheManager *>(Engine::get_singleton()->get_singleton_object("FileCacheManager"))->get_sss();
		ERR_FAIL_COND(!cache_mgr);
		sem = Semaphore::create();
		ERR_FAIL_COND(!sem);
	}

	virtual ~FileAccessCached() {

		memdelete(sem);
		if(cached_file.is_valid())
			cache_mgr->close(cached_file);
	}
};

class _FileAccessCached: public Object {

	GDCLASS(_FileAccessCached, Object);

	friend class FileAccessCached;

protected:

	FileAccessCached fac;

	static void _bind_methods() {
		ClassDB::bind_method(D_METHOD("open", "path", "mode"), &_FileAccessCached::open);
		ClassDB::bind_method(D_METHOD("close"), &_FileAccessCached::close);
		ClassDB::bind_method(D_METHOD("get_buffer", "len"), &_FileAccessCached::get_buffer);
	}

public:

	_FileAccessCached() {}

	_FileAccessCached *open(Variant path, Variant mode) {

		if(fac._open(String(path), int(mode)) == OK) {
			return this;
		} else return NULL;
	}

	PoolByteArray get_buffer(int len) {
		PoolByteArray pba = PoolByteArray();
		PoolByteArray::Write w = pba.write();
		if (fac.get_buffer(w.ptr(), len) < len) {
			WARN_PRINT(("Got less than " + itos(len) + " bytes.").utf8().get_data());
		}
		return pba;
	}

	void close() {

		if(fac.is_open())
			fac.close();
	}

};

#endif // FILE_ACCESS_CACHED_H
