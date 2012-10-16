/*
 * distquicklib: library of concurrent and distributed quicksort
 * algorithms for COMP2310 Assignment 2, 2012.
 *
 * Name: StudentId:
 *
 * ***Disclaimer***: (modify as appropriate) The work that I am submitting
 * for this program is without significant contributions from others
 * (excepting course staff).
 */

// uncomment when debugging. Example call: PRINTF(("x=%d\n", x));
// #define PRINTF(x) do { printf x; fflush(stdout); } while (0)
#define PRINTF(x)               /* use when not debugging */

#include <stdio.h>
#include <stdlib.h>             /* malloc, free */
#include <strings.h>            /* bcopy() */
#include <assert.h>
#include <unistd.h>             /* fork(), pipe() */
#include <sys/types.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>


#include "quicklib.h"
#include "distquicklib.h"
#include "dbg.h"
#include "utils.h"


/*
 * Macros
 */
#define min(m,n) ((m) < (n) ? (m) : (n))
#define max(m,n) ((m) > (n) ? (m) : (n))

/*
 * Prototypes
 */
ssize_t         read_all_ints(int fildes, int *buf, int ntimes);

ssize_t         write_all_ints(int fildes, int *buf, int ntimes);


ssize_t
read_all_ints(int fildes, int *buf, int ntimes)
{
    int             num_total_ints_read,
                    num_ints_left,
                    bytes_read,
                    ints_read;

    num_total_ints_read = 0;
    num_ints_left = ntimes;

    while (num_total_ints_read < ntimes) {
        bytes_read =
            read(fildes, buf + num_total_ints_read,
                 sizeof(int) * min(num_ints_left, 20000));
        check(bytes_read > 0, "Cannot read");
        ints_read = bytes_read / sizeof(int);
        num_total_ints_read += ints_read;
        num_ints_left -= ints_read;
    }
    return num_total_ints_read;

  error:
    return -1;
}


ssize_t
write_all_ints(int fildes, int *buf, int ntimes)
{
    int             num_total_ints_written,
                    num_ints_left,
                    bytes_written,
                    ints_written;

    num_total_ints_written = 0;
    num_ints_left = ntimes;

    while (num_total_ints_written < ntimes) {
        bytes_written =
            write(fildes, buf + num_total_ints_written,
                  sizeof(int) * min(num_ints_left, 10000));
        check(bytes_written > 0, "Cannot write");
        ints_written = bytes_written / sizeof(int);
        num_total_ints_written += ints_written;
        num_ints_left -= ints_written;
    }

    return num_total_ints_written;

  error:
    return -1;
}


// distributed quick sort using pipes
void
quickPipe(int A[], int n, int p)
{
    int             index,
                    left_size,
                    right_size,
                   *buffer,
                    i,
                    size,
                    tag,
                    last_tag_used,
                    new_tag,
                    m,
                    num_children_created,
                    child_id,
                    child_tag;

    int             fdmax;
    fd_set          read_fds;

    setbuf(stdout, NULL);

    int             k = lg2(p);

    int             child_id_to_tag[k];

    // File descriptors
    int             fd_pc[k][2],
                    fd_cp[k][2],
                    fd_reply;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;

    int             offset_table[k],
                    size_table[k];

    num_children_created = 0;
    fd_reply = -1;

    size = n;
    buffer = A;
    child_id = 0;

    tag = 0;
    last_tag_used = p + 1;

    log_info("Input:");
    printArray(A, n);

    while (p != 1) {
        // Be practical!
        // if (size <= 10) {
        // break;
        // }
        new_tag = (tag + last_tag_used) / 2;
        m = partition(buffer, size);
        left_size = m + 1;
        right_size = size - m - 1;
        index = m + 1;

        // Do not be absurd.
        if (left_size <= 0 || right_size <= 0) {
            break;
        }

        check(pipe(fd_cp[child_id]) == 0, "%d:\tCannot pipe().", tag);
        check(pipe(fd_pc[child_id]) == 0, "%d:\tCannot pipe().", tag);

        log_info("%d:\tPrepare to create %d", tag, new_tag);

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;
            // break;
        case 0:
            log_info("%d:\tIs created.", new_tag);

            // Information
            tag = new_tag;
            size = right_size;

            buffer = malloc(right_size * sizeof(int));
            check_mem(buffer);

            log_info
                ("%d:\tPrepare to receive %d elements.", tag, right_size);
            num_ints_read = read_all_ints(fd_pc[child_id][0], buffer, right_size);
            check(num_ints_read != -1,
                  "The child cannot read from the parent");
            log_info("%d:\tReceived: %ld.", tag, num_ints_read);
            printArray(buffer, right_size);

            // file descriptors
            fd_reply = dup(fd_cp[child_id][1]);

            // The child cannot reuse any filedescriptor except
            // fd_cp[child_id][1]
            for (i = 0; i < k; ++i) {
                CLOSEFD(fd_cp[i][1]);
                CLOSEFD(fd_cp[i][0]);
                CLOSEFD(fd_pc[i][1]);
                CLOSEFD(fd_pc[i][0]);
            }

            child_id = 0;
            num_children_created = 0;
            break;

        default:
            // Parent does this.
            log_info("%d:\tis left with %d elements:", tag, left_size);
            printArray(buffer, left_size);

            size_table[child_id] = right_size;
            offset_table[child_id] = index;
            child_id_to_tag[child_id] = new_tag;
            last_tag_used = new_tag;
            size = left_size;

            num_ints_written = write_all_ints(fd_pc[child_id][1], &buffer[m + 1],
                                              right_size);
            check(num_ints_written != -1,
                  "%d:\tCannot write %d to the pipe.", tag, left_size);
            log_info("%d:\tHas sent %d elments to %d", tag, right_size,
                     new_tag);

            CLOSEFD(fd_pc[child_id][0]);
            CLOSEFD(fd_pc[child_id][1]);
            CLOSEFD(fd_cp[child_id][1]);
            ++num_children_created;
            ++child_id;
            break;
        }
        p /= 2;
    }

    for (i = 0; i < k; ++i) {
        CLOSEFD(fd_pc[i][0]);
        CLOSEFD(fd_pc[i][1]);
        CLOSEFD(fd_cp[i][1]);
        if (tag % 2 != 0) {
            CLOSEFD(fd_cp[i][0]);
        }
    }

    quickSort(buffer, size);

    log_info("%d\tHas done:", tag);
    printArray(buffer, size);

    struct timeval  tv;
    tv.tv_sec = 4;
    tv.tv_usec = 500000;
    int             t = 0;

    if (tag % 2 == 0) {
        if (num_children_created == 0) {
            log_info("%d\tis waiting for reply", tag);
        }
        // check(fdmax != -1, "Panic");
        while (num_children_created > 0) {
            fdmax = -1;
            FD_ZERO(&read_fds);
            for (i = 0; i < child_id; ++i) {
                int             fd;
                fd = fd_cp[i][0];
                if (fd == -1) {
                    continue;
                }
                FD_SET(fd, &read_fds);
                fdmax = max(fdmax, fd);
            }
            ++fdmax;

            select(fdmax, &read_fds, NULL, NULL, &tv);

            log_info("%d:\tis unblockded", tag);
            t = 0;
            for (i = 0; i < child_id; ++i) {
                int             fd = fd_cp[i][0];
                if (fd == -1 || fd >= fdmax) {
                    continue;
                }
                if (FD_ISSET(fd, &read_fds)) {
                    ++t;
                    child_tag = child_id_to_tag[i];
                    log_info
                        ("%d:\tReading %d elements, offset %d, from: %d",
                         tag, size_table[i], offset_table[i], i);
                    num_ints_read =
                        read_all_ints(fd,
                                      buffer + offset_table[i],
                                      size_table[i]);
                    check(num_ints_read != -1, "Panic %d, %d %d", tag,
                          child_tag, fd_cp[i][0]);
                    check(num_ints_read == size_table[i],
                          "In %d %d Cannot read from %d. Is %ld, should be %d",
                          tag, i, child_tag, num_ints_read / sizeof(int),
                          size_table[i]);
                    size += num_ints_read;
                    log_info
                        ("%d\tHas read %ld elements from %d at offset %d:",
                         tag, num_ints_read, i, offset_table[i]);
                    printArray(buffer + offset_table[i], size_table[i]);
                    // wait(NULL);
                    CLOSEFD(fd_cp[i][0]);
                    --num_children_created;
                }
            }

            if (t == 0) {
                log_info("%d is deadlock with elements: %d", tag, size);
                _exit(EXIT_FAILURE);
            }
        }
    }

    if (tag != 0) {
        log_info("%d:\tSending %d elements to its parent", tag, size);
        num_ints_written = write_all_ints(fd_reply, buffer, size);
        check(num_ints_written == size, "Cannot write to %d", fd_reply);
        log_info("%d:\tSent %d elements to its parent", tag, size);
    }


    if (buffer != A)
        free(buffer);

    while (child_id != 0) {
        log_info("%d:\tWaiting, still has %d children", tag, child_id);
        wait(NULL);
        --child_id;
    }

    if (tag == 0) {
        printArray(A, n);
        return;
    } else {
        _exit(EXIT_SUCCESS);
    }

  error:
    for (i = 0; i < k; ++i) {
        CLOSEFD(fd_pc[i][0]);
        CLOSEFD(fd_pc[i][1]);
        CLOSEFD(fd_cp[i][0]);
        CLOSEFD(fd_cp[i][1]);
    }

    if (tag == 0) {
        return;
    } else {
        _exit(EXIT_FAILURE);
    }
}


// distributed quick sort using sockets
void
quickSocket(int A[], int n, int p)
{

}



// concurrent quick sort using pthreads
void
quickThread(int *pA, int pn, int p, enum WaitMechanismType pWaitMech)
{
}
