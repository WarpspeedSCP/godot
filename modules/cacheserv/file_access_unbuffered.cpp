#include "file_access_unbuffered.h"

#if defined(UNIX_ENABLED) || defined(LIBC_FILEIO_ENABLED)

#include "core/os/os.h"
#include "core/print_string.h"

#include <sys/stat.h>
#include <sys/types.h>

#if defined(UNIX_ENABLED)
#include <unistd.h>
#include <fcntl.h>
#endif

#ifndef ANDROID_ENABLED
#include <sys/statvfs.h>
#endif

#ifdef MSVC
#define S_ISREG(m) ((m)&_S_IFREG)
#include <io.h>
#endif
#ifndef S_ISREG
#define S_ISREG(m) ((m)&S_IFREG)
#endif

// Stubbed because of lack of utility.
void FileAccessUnbuffered::check_errors() const {
}

void FileAccessUnbuffered::check_errors(int val, int expected, int mode) {

	ERR_FAIL_COND(fd < 0);

	switch(mode) {
		case CHK_MODE_SEEK:
			if (val >= st.st_size) {
				last_error = ERR_FILE_EOF;
			} else if (val != expected) {
				ERR_PRINT(("Read less than " + itos(expected) + " bytes").utf8().get_data());
			}
			return;
		case CHK_MODE_WRITE:
			if (val == -1) {
				ERR_PRINT(("Write error with file." + this->path).utf8().get_data());
				last_error = ERR_FILE_CANT_WRITE;
			}
			return;
		case CHK_MODE_READ:
			if(val == -1) {
				ERR_PRINT(("Read error with file." + this->path).utf8().get_data());
				last_error = ERR_FILE_CANT_READ;
			}
			return;
	}




}

Error FileAccessUnbuffered::_open(const String &p_path, int p_mode_flags) {

	if (fd < 0)
		::close(fd);
	fd = -1;

	path_src = p_path;
	path = fix_path(p_path);
	//printf("opening %ls, %i\n", path.c_str(), Memory::get_static_mem_usage());

	ERR_FAIL_COND_V(fd != -1, ERR_ALREADY_IN_USE);
	int mode;

	if (p_mode_flags == READ)
		mode = O_RDONLY | O_SYNC;
	else if (p_mode_flags == WRITE)
		mode = O_WRONLY | O_TRUNC | O_CREAT | O_SYNC;
	else if (p_mode_flags == READ_WRITE)
		mode = O_RDWR | O_SYNC;
	else if (p_mode_flags == WRITE_READ)
		mode = O_RDWR | O_TRUNC | O_CREAT | O_SYNC;
	else
		return ERR_INVALID_PARAMETER;

	#ifdef O_DIRECT
		mode |= O_DIRECT;
	#endif

	/* pretty much every implementation that uses fopen as primary
	   backend (unix-compatible mostly) supports utf8 encoding */

	//printf("opening %s as %s\n", p_path.utf8().get_data(), path.utf8().get_data());
	int err = stat(path.utf8().get_data(), &st);
	if (!err) {
		switch (st.st_mode & S_IFMT) {
			case S_IFLNK:
			case S_IFREG:
				break;
			default:
				return ERR_FILE_CANT_OPEN;
		}
	}

	if (is_backup_save_enabled() && (p_mode_flags & WRITE) && !(p_mode_flags & READ)) {
		save_path = path;
		path = path + ".tmp";
	}

	fd = ::open(path.utf8().get_data(), mode);

	if (fd < 0) {
		last_error = ERR_FILE_CANT_OPEN;
		return ERR_FILE_CANT_OPEN;
	} else {
		last_error = OK;
		flags = p_mode_flags;
		return OK;
	}
}

void FileAccessUnbuffered::close() {

	if (fd < 0)
		return;

	::close(fd);
	fd = -1;

	if (close_notification_func) {
		close_notification_func(path, flags);
	}

	if (save_path != "") {
		int rename_error = rename((save_path + ".tmp").utf8().get_data(), save_path.utf8().get_data());

		if (rename_error && close_fail_notify) {
			close_fail_notify(save_path);
		}

		save_path = "";
		ERR_FAIL_COND(rename_error != 0);
	}
}

bool FileAccessUnbuffered::is_open() const {

	return (fd < 0);
}

String FileAccessUnbuffered::get_path() const {

	return path_src;
}

String FileAccessUnbuffered::get_path_absolute() const {

	return path;
}

void FileAccessUnbuffered::seek(size_t p_position) {

	ERR_FAIL_COND(fd < 0);
	ERR_FAIL_COND(p_position < 0);

	last_error = OK;

	int new_pos = ::lseek(fd, p_position, SEEK_SET);

	ERR_FAIL_COND(new_pos == -1);

	check_errors(new_pos, p_position, CHK_MODE_SEEK);

	if(new_pos >= st.st_size) {

		pos = ::lseek(fd, 0, SEEK_END);

	} else {

		pos = new_pos;

	}

}

void FileAccessUnbuffered::seek_end(int64_t p_position) {

	ERR_FAIL_COND(fd < 0);
	ERR_FAIL_COND(p_position > 0);

	last_error = OK;

	int new_pos = ::lseek(fd, p_position, SEEK_END);

	ERR_FAIL_COND(new_pos == -1);

	check_errors(new_pos, st.st_size - p_position, CHK_MODE_SEEK);

	pos = new_pos;
}

size_t FileAccessUnbuffered::get_position() const {

	ERR_FAIL_COND_V(fd < 0, 0);

	long pos = ::lseek(fd, 0, SEEK_CUR);
	if (pos < 0) {
		check_errors();
		ERR_FAIL_V(0);
	}
	return pos;
}

size_t FileAccessUnbuffered::get_len() const {

	ERR_FAIL_COND_V(fd < 0, 0);

	// long pos = ::lseek(fd, 0, SEEK_CUR);
	// ERR_FAIL_COND_V(pos < 0, 0);
	// ERR_FAIL_COND_V(::lseek(fd, 0, SEEK_END), 0);
	struct stat st;
	ERR_FAIL_COND_V(fstat(fd, &st) < 0, 0);
	// ERR_FAIL_COND_V(fseek(f, pos, SEEK_SET), 0);

	return st.st_size;
}

size_t FileAccessUnbuffered::get_len() {

	ERR_FAIL_COND_V(fd < 0, 0);
	ERR_FAIL_COND_V(fstat(fd, &st) < 0, 0);

	return st.st_size;
}

bool FileAccessUnbuffered::eof_reached() const {

	return last_error == ERR_FILE_EOF;
}

uint8_t FileAccessUnbuffered::get_8() const {

	ERR_FAIL_COND_V(fd < 0, 0);
	uint8_t b;
	if (::read(fd, &b, 1) < 1) {
		check_errors();
		b = '\0';
	}
	return b;
}

int FileAccessUnbuffered::get_buffer(uint8_t *p_dst, int p_length) const {

	return get_buffer(p_dst, p_length);
};

int FileAccessUnbuffered::get_buffer(uint8_t *p_dst, int p_length) {

	// TODO: fix all.
	ERR_FAIL_COND_V(fd < 0, -1);
	int n_last_read = ::read(fd, p_dst, p_length);
	check_errors();
	return n_last_read;
};

Error FileAccessUnbuffered::get_error() const {

	return last_error;
}

void FileAccessUnbuffered::store_8(uint8_t p_byte) {

	ERR_FAIL_COND(fd < 0);
	ERR_FAIL_COND(write(fd, &p_byte, 1) != 1);
}

void FileAccessUnbuffered::store_buffer(const uint8_t *p_src, int p_length) {
	ERR_FAIL_COND(fd < 0);
	ERR_FAIL_COND((int)write(fd, p_src, p_length) != p_length);
}

bool FileAccessUnbuffered::file_exists(const String &p_path) {

	int err;
	String filename = fix_path(p_path);

	// Does the name exist at all?
	err = stat(filename.utf8().get_data(), &st);
	if (err)
		return false;

#ifdef UNIX_ENABLED
	// See if we have access to the file
	if (access(filename.utf8().get_data(), F_OK))
		return false;
#else
	if (_access(filename.utf8().get_data(), 4) == -1)
		return false;
#endif

	// See if this is a regular file
	switch (st.st_mode & S_IFMT) {
		case S_IFLNK:
		case S_IFREG:
			return true;
		default:
			return false;
	}
}

uint64_t FileAccessUnbuffered::_get_modified_time(const String &p_file) {

	String file = fix_path(p_file);
	int err = stat(file.utf8().get_data(), &st);

	if (!err) {
		return st.st_mtime;
	} else {
		ERR_EXPLAIN("Failed to get modified time for: " + p_file);
		ERR_FAIL_V(0);
	};
}

Error FileAccessUnbuffered::_chmod(const String &p_path, int p_mod) {
	int err = chmod(p_path.utf8().get_data(), p_mod);
	if (!err) {
		return OK;
	}

	return FAILED;
}


// Flush does not make sense for unbuffered IO so it has only checks and does not actually do anything.
void FileAccessUnbuffered::flush() {

	ERR_FAIL_COND(fd < 0);

}

FileAccess *FileAccessUnbuffered::create_unbuf_unix() {

	return memnew(FileAccessUnbuffered);
}

CloseNotificationFunc FileAccessUnbuffered::close_notification_func = NULL;

FileAccessUnbuffered::FileAccessUnbuffered() :
		fd(-1),
		flags(0),
		last_error(OK) {
}

FileAccessUnbuffered::~FileAccessUnbuffered() {

	close();
}

#endif