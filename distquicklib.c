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

#include "quicklib.h"
#include "distquicklib.h"
#include "dbg.h"
#include "utils.h"


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
                    offset,
                    tmp,
                    tag,
                    last,
                    new_tag,
                    m;

    int             fdmax;
    fd_set          master,
                    read_fds;

    setbuf(stdout, NULL);

    // File descriptors
    int             fd_pc[2],
                    fd_cp[p][2];

    // Status code
    ssize_t         bytes_read,
                    bytes_written;

    int             num_fds = 3 + 2 * p * 2;

    int             offset_table[num_fds],
                    size_table[num_fds];

    fd_pc[0] = -1;
    fd_pc[1] = -1;

    FD_ZERO(&master);
    fdmax = -1;
    for (i = 1; i < p; ++i) {
        fd_cp[i][0] = -1;
        fd_cp[i][1] = -1;
        check(pipe(fd_cp[i]) == 0, "Cannot pipe().");
        FD_SET(fd_cp[i][0], &master);
        if (fdmax < fd_cp[i][0]) {
            fdmax = fd_cp[i][0];
        }
    }
    ++fdmax;

    for (i = 0; i < num_fds; ++i) {
        offset_table[i] = -1;
        size_table[i] = -1;
    }

    offset = 0;
    size = n;
    buffer = A;

    tag = 0;
    last = p + 1;


    // printArray(A, n);

    while (p != 1) {
        // Be practical!
        // if (size <= 10)
        // break;
        new_tag = (tag + last) / 2;
        m = partition(buffer, size);
        left_size = m + 1;
        right_size = size - m - 1;
        index = m + 1;
        // Do not be absurd.
        if (left_size <= 10 || right_size <= 10)
            break;
        check(pipe(fd_pc) == 0, "Cannot pipe().");
        log_info("%d is prepare to create %d", tag, new_tag);
        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;
            // break;
        case 0:
            // Child does this.
            tag = new_tag;
            size = right_size;
            offset += index;
            for (i = 0; i < p; ++i) {
                CLOSEFD(fd_cp[i][0]);
            }
            buffer = malloc(right_size * sizeof(int));
            check_mem(buffer);
            log_info
                ("%d is initialised and prepare to receive %ld elements %ld bytes of data from the parent:",
                 tag, right_size, right_size * sizeof(int));
            bytes_read = read(fd_pc[0], buffer, right_size * sizeof(int));
            check(bytes_read != -1,
                  "The child cannot read from the parent");
            log_info("%d is initialised with element: %d.", tag,
                     bytes_read / sizeof(int));
            CLOSEFD(fd_pc[1]);
            CLOSEFD(fd_pc[0]);
            break;
        default:
            // Parent does this.
            log_info("The parent of %d is left with %d bytes %d elments",
                     new_tag, left_size * sizeof(int), left_size);
            last = new_tag;
            size = left_size;
            bytes_written = write(fd_pc[1], &buffer[m + 1],
                                  right_size * sizeof(int));
            check(bytes_written != -1,
                  "The parent cannot write to the pipe.");
            log_info("The parent has sent %d elments to %d", right_size,
                     new_tag);
            CLOSEFD(fd_pc[0]);
            CLOSEFD(fd_pc[1]);
            break;
        }
        p /= 2;
    }

    quickSort(buffer, size);


    if (tag == 0) {
        log_info("The root is listening for reply");
        while (1) {
            if (size == n) {
                break;
            }
            read_fds = master;
            select(fdmax, &read_fds, NULL, NULL, NULL);
            log_info("The root is unblocked");
            for (i = 0; i < fdmax; ++i) {
                if (FD_ISSET(i, &read_fds)) {
                    log_info("The root is reading from %d", i);
                    if (size_table[i] == -1) {
                        log_info("The root is reading size from %d", i);
                        bytes_read = read(i, &tmp, sizeof tmp);
                        check(bytes_read == sizeof tmp, "Cannot read");
                        size_table[i] = tmp;
                        if (tmp == 0)
                            FD_CLR(i, &master);
                        log_info("The root has size from %d: %d", i, tmp);
                    } else if (offset_table[i] == -1) {
                        log_info("The root is reading offset from %d", i);
                        bytes_read = read(i, &tmp, sizeof tmp);
                        check(bytes_read == sizeof tmp, "Cannot read");
                        offset_table[i] = tmp;
                        log_info("The root has offset from %d: %d", i,
                                 tmp);
                    } else {
                        log_info
                            ("The root is reading data at the offset: %d from: %d",
                             offset_table[i], i);
                        bytes_read =
                            read(i, A + offset_table[i],
                                 size_table[i] * sizeof(int));
                        check(bytes_read == size_table[i] * sizeof(int),
                              "%d Cannot read. Is %ld, should be %d", i,
                              bytes_read / sizeof(int), size_table[i]);
                        size += size_table[i];
                        FD_CLR(i, &master);
                        log_info
                            ("The root has read %ld bytes of data from %d. Now root has %d elements",
                             bytes_read, i, size);
                        // printArray(A, n);
                    }
                }
            }
        }
    } else {
        // log_info("%d has done:", tag);
        // printArray(buffer, size);
        bytes_written = write(fd_cp[tag][1], &size, sizeof size);
        check(bytes_written == sizeof size, "Cannot write size");
        if (size != 0) {
            bytes_written = write(fd_cp[tag][1], &offset, sizeof offset);
            check(bytes_written == sizeof offset,
                  "%d Cannot write offset %d", tag, fd_cp[tag][1]);
            log_info("%d is sending %d elements to the root", tag, size);
            bytes_written =
                write(fd_cp[tag][1], buffer, size * sizeof(int));
            check(bytes_written == size * sizeof(int),
                  "Cannot write size");
            log_info("%d has sent %d elements to the root", tag, size);
        }
    }

    for (i = 0; i < p; ++i) {
        fd_pc[0] = -1;
        fd_pc[1] = -1;
        fd_cp[i][0] = -1;
        fd_cp[i][1] = -1;
    }

    if (tag == 0) {
        // printArray(A, n);
        return;
    } else {
        _exit(EXIT_SUCCESS);
    }

  error:
    for (i = 0; i < p; ++i) {
        fd_pc[0] = -1;
        fd_pc[1] = -1;
        fd_cp[i][0] = -1;
        fd_cp[i][1] = -1;
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
