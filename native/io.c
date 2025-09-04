#include <liburing/io_uring.h>
#include <linux/fs.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <liburing.h>


typedef struct{
    unsigned char* pointer;
    u_int64_t offset ;
} WriteElement;
typedef struct {
    size_t order;
    size_t max;
} bucket_instance;

extern int batch_write(size_t buffer_size, size_t buffer_length, WriteElement* entries, int fd);


extern int batch_write(size_t buffer_size, size_t buffer_length, WriteElement* entries, int fd);


int batch_write(size_t buffer_size, size_t buffer_length, WriteElement* entries, int fd) {
    struct io_uring ring;
    if (io_uring_queue_init(buffer_length + 1, &ring, 0) < 0) {
        return -1;
    }
    
    int product = -1;
    
    for (size_t i = 0; i < buffer_length; i++) {
        WriteElement el = entries[i];
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe) goto clean;
        io_uring_prep_write(sqe, fd, el.pointer, buffer_size, el.offset);
        sqe->flags |= IOSQE_IO_HARDLINK;
    }
   
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) goto clean;
    io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
    io_uring_submit(&ring);
    
    
    for (size_t i = 0; i < buffer_length + 1; i++) {
        struct io_uring_cqe* cqe;
        int result = io_uring_wait_cqe(&ring, &cqe);
        io_uring_cqe_seen(&ring,cqe);
        if (result < 0 || cqe->res < 0) goto clean;
    }
    
    product = 0;
    
    clean:
        io_uring_queue_exit(&ring);
        return product;
}
