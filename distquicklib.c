/**
 * distquicklib: library of concurrent and distributed quicksort
 * algorithms for COMP2310 Assignment 2, 2012.
 *
 * Name: Meitian Huang
 *
 * StudentId: u4700480
 *
 * ***Disclaimer***: (modify as appropriate) The work that I am submitting
 * for this program is without significant contributions from others
 * (excepting course staff).
 */

/*-
 * Copyright (c) 2012 Meitian Huang <_@freeaddr.info>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

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

/**
 * Debug Macros
 **/

#include <errno.h>
#include <string.h>

#define clean_errno()           (errno == 0 ? "None" : strerror(errno))

#define log_err(M, ...)         do {                                          \
                                    fprintf(stderr,                           \
                                            "[ERROR] (%s:%d: errno: %s) " M   \
                                            "\n", __FILE__, __LINE__,         \
                                            clean_errno(), ##__VA_ARGS__);    \
                                } while (0)

#define log_warn(M, ...)        do {                                          \
                                    fprintf(stderr,                           \
                                            "[WARN] (%s:%d: errno: %s) " M    \
                                            "\n", __FILE__, __LINE__,         \
                                            clean_errno(), ##__VA_ARGS__);    \
                                } while (0)

#ifdef DEBUG
#define log_info(M, ...)        do {                                          \
                                    fprintf(stderr, "[INFO] (%s:%d) " M       \
                                    "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
                                } while (0)
#else
#define log_info(M, ...)
#endif

#define check(A, M, ...)        do {                                          \
                                    if(!(A)) {                                \
                                        log_err(M, ##__VA_ARGS__);            \
                                        errno=0;                              \
                                        goto error;                           \
                                    }                                         \
                                } while (0)

/*
 * Macros
 */

#define min(m,n)                ((m) < (n) ? (m) : (n))

#define max(m,n)                ((m) > (n) ? (m) : (n))

#define ALLOWANCE               1000

#define THRESHOLD               10

/*
 * Shorthand
 */
#define check_arguments(A, n, p)                                              \
                                do {                                          \
                                    check(A != NULL, "A is NULL.");           \
                                    check(n >= 0, "Don't be negative, man."); \
                                    check(p >= 1, "Don't be absurd.");        \
                                } while (0)

#define check_partition(n, m, left_size, right_size)                          \
                                do {                                          \
                                    check(m >= 0 && m <= n &&                 \
                                          left_size >= 0 && left_size <= n && \
                                          right_size >= 0 && right_size <= n, \
                                          "Something is wrong. "              \
                                          "Yet, it is not my fault. "         \
                                          "Ask Peter");                       \
                                } while (0)

#define CLOSEFD(fd)             do {                                          \
                                    if (fd != -1) {                           \
                                        close(fd);                            \
                                    }                                         \
                                } while (0)

#ifdef DEBUG
#define debug_printArray(A, n)  printArray(A, n)
#else
#define debug_printArray(A, n)
#endif

/**
 * Prototypes
 **/
static ssize_t  read_all_ints(int fildes, int *buf, int ntimes);

static ssize_t  write_all_ints(int fildes, int *buf, int ntimes);

/**
 * A wrapper function of read(2). The unit of its arguments is ints, not bytes.
 *
 * Including this function has two benefits:
 *
 * 1. Avoiding putting bytes-to-ints-conversions all over the code.
 *
 * 2. One may think read(2) and write(2) fail iff they return -1. Yet, one
 *    should notice that read(2) and write(2) return the number of bytes which
 *    were read or written returned upon successful completion. This is very
 *    likely to happen if one tries to read(2) or write(2) a big chunk of data
 *    to a socket. This function will attempt to handle the partial read(2).
 **/
static          ssize_t
read_all_ints(int fildes, int *buf, int ntimes)
{
    ssize_t         num_total_ints_read,
                    num_ints_left,
                    bytes_read,
                    ints_read;

    check(fildes != -1, "Invalid file descriptor.");
    check(buf != NULL, "Invalid address to write.");
    check(ntimes >= 0, "I do not know how to read %d integers", ntimes);

    num_total_ints_read = 0;
    num_ints_left = ntimes;

    while (num_total_ints_read < ntimes) {
        bytes_read = read(fildes, buf + num_total_ints_read,
                          sizeof(int) * min(num_ints_left, ALLOWANCE));
        check(bytes_read > 0, "Cannot read");
        ints_read = bytes_read / sizeof(int);
        num_total_ints_read += ints_read;
        num_ints_left -= ints_read;
    }
    return num_total_ints_read;

  error:
    return -1;
}

/**
 * Ditto
 **/
static          ssize_t
write_all_ints(int fildes, int *buf, int ntimes)
{
    ssize_t         num_total_ints_written,
                    num_ints_left,
                    bytes_written,
                    ints_written;

    check(fildes != -1, "Invalid file descriptor.");
    check(buf != NULL, "Invalid address to write.");
    check(ntimes >= 0, "I do not know how to write %d integers", ntimes);

    num_total_ints_written = 0;
    num_ints_left = ntimes;

    while (num_total_ints_written < ntimes) {
        bytes_written = write(fildes, buf + num_total_ints_written,
                              sizeof(int) * min(num_ints_left, ALLOWANCE));
        check(bytes_written > 0, "Cannot write");
        ints_written = bytes_written / sizeof(int);
        num_total_ints_written += ints_written;
        num_ints_left -= ints_written;
    }

    return num_total_ints_written;

  error:
    return -1;
}

/**
 * distributed quick sort using pipes
 **/
void
quickPipe(int A[], int n, int p)
{
    int             index,
                    left_size,
                    right_size,
                    m,
                   *ptr;

    int             fd_cp[2];

    ssize_t         num_ints_read,
                    num_ints_written;

    check_arguments(A, n, p);

    fd_cp[0] = -1;
    fd_cp[1] = -1;

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    if (p == 1 || n <= THRESHOLD) {
        quickSort(A, n);
    } else {
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;

        check_partition(n, m, left_size, right_size);

        ptr = &(A[index]);

        check(pipe(fd_cp) == 0, "Cannot pipe().");

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;

        case 0:
            CLOSEFD(fd_cp[0]);

            quickPipe(ptr, right_size, p / 2);
            num_ints_written = write_all_ints(fd_cp[1], ptr, right_size);
            check(num_ints_written == right_size, "Cannot write.");

            CLOSEFD(fd_cp[1]);
            _exit(EXIT_SUCCESS);

        default:
            CLOSEFD(fd_cp[1]);

            quickPipe(A, left_size, p / 2);
            num_ints_read = read_all_ints(fd_cp[0], ptr, right_size);
            check(num_ints_read == right_size, "Cannot read.");

            CLOSEFD(fd_cp[0]);
            wait(NULL);
            break;
        }
    }
    return;

  error:
    CLOSEFD(fd_cp[0]);
    CLOSEFD(fd_cp[1]);
    return;
}

// distributed quick sort using sockets
void
quickSocket(int A[], int n, int p)
{
    int             index,
                    left_size,
                    right_size,
                    m,
                   *ptr;

    // File descriptors
    int             fd_listener,
                    fd_p,
                    fd_c;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;
    int             status;

    struct sockaddr_in server = {
        .sin_family = AF_INET,
        .sin_port = 0,
        .sin_addr.s_addr = INADDR_ANY
    };

    socklen_t       namelen;

    fd_listener = -1;
    fd_p = -1;
    fd_c = -1;

    check_arguments(A, n, p);

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    namelen = sizeof(server);

    if (p == 1 || n <= THRESHOLD) {
        quickSort(A, n);
    } else {
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;

        check_partition(n, m, left_size, right_size);

        ptr = &(A[index]);

        fd_listener = socket(AF_INET, SOCK_STREAM, 0);
        check(fd_listener != -1, "no socket");
        status = bind(fd_listener, (struct sockaddr *) &server, namelen);
        check(status != -1, "Cannot bind(2).");
        status =
            getsockname(fd_listener, (struct sockaddr *) &server,
                        &namelen);
        check(status != -1, "Cannot getsockname(2).");
        status = listen(fd_listener, 1);
        check(status == 0, "Cannot listen(2).");

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;

        case 0:
            CLOSEFD(fd_p);
            CLOSEFD(fd_listener);

            quickSocket(ptr, right_size, p / 2);

            fd_c = socket(AF_INET, SOCK_STREAM, 0);
            check(fd_c != -1, "Cannot create a socket");
            status = connect(fd_c, (struct sockaddr *) &server, namelen);
            check(status != -1, "Cannot connect");
            num_ints_written = write_all_ints(fd_c, ptr, right_size);
            check(num_ints_written == right_size, "Cannot write");

            CLOSEFD(fd_c);
            _exit(EXIT_SUCCESS);

        default:
            CLOSEFD(fd_c);

            quickSocket(A, left_size, p / 2);

            fd_p = accept(fd_listener, NULL, NULL);
            check(fd_p != -1, "Cannot accept the connect from child.");
            num_ints_read = read_all_ints(fd_p, ptr, right_size);
            check(num_ints_read == right_size, "Cannot read");

            CLOSEFD(fd_listener);
            CLOSEFD(fd_p);
            return;
        }
    }

    /*
     * Not reacheable
     */

  error:
    CLOSEFD(fd_c);
    CLOSEFD(fd_p);
    CLOSEFD(fd_listener);
    return;
}

/*
 * Contains _all_ information required in each thread.
 */
struct info {
    int            *A;          /* Start of the array to be sorted */
    int             n;          /* No. of elements to be sorted */
    int             p;          /* No. of threads allowed */
    int             num_pending_children;       /* Num of pending children 
                                                 */
    volatile int    done;       /* Set if the child has done */
    /*
     * Here may be a padding (8 bytes)
     */
    pthread_mutex_t mutex;
};

static void    *
thread_routine_join(void *info)
{
    struct info    *in,
                    ch;
    int             m,
                    index,
                    left_size,
                    right_size;
    int             status;
    void           *res;
    pthread_t       thread;

    check(info != NULL, "Invalid argument");
    in = (struct info *) info;
    check_arguments(in->A, in->n, in->p);

    if (in->p == 1 || in->n <= THRESHOLD) {
        quickSort(in->A, in->n);
    } else {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;

        check_partition(in->n, m, left_size, right_size);

        ch.A = &(in->A[index]);
        ch.n = right_size;
        ch.p = in->p / 2;
        status = pthread_create(&thread, NULL, thread_routine_join, &ch);
        check(status == 0, "Cannot create thread");

        in->n = left_size;
        in->p /= 2;

        thread_routine_join(in);
        status = pthread_join(thread, &res);
        check(status == 0, "Cannot join thread");
    }
    return NULL;

  error:
    return NULL;
}

static void    *
thread_routine_mutex(void *info)
{
    struct info    *in,
                    ch;
    int             m,
                    index,
                    left_size,
                    right_size;
    int             status;
    pthread_t       thread;

    check(info != NULL, "Invalid argument");
    in = (struct info *) info;
    check_arguments(in->A, in->n, in->p);

    if (in->p == 1 || in->n <= THRESHOLD) {
        quickSort(in->A, in->n);
    } else {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;

        check_partition(in->n, m, left_size, right_size);

        ch.A = &(in->A[index]);
        ch.n = right_size;
        ch.p = in->p / 2;
        ch.num_pending_children = 0;

        status = pthread_mutex_init(&(ch.mutex), NULL);
        check(status == 0, "Cannot initialise a mutex");
        status = pthread_mutex_lock(&(ch.mutex));
        check(status == 0, "Cannot acquire the lock");

        status = pthread_create(&thread, NULL, thread_routine_mutex, &ch);
        check(status == 0, "Cannot create thread");

        in->n = left_size;
        in->p /= 2;

        ++(in->num_pending_children);
        thread_routine_mutex(in);
        --(in->num_pending_children);

        status = pthread_mutex_lock(&(ch.mutex));
        check(status == 0, "Cannot wait");
        pthread_mutex_destroy(&(ch.mutex));
    }

    // Release the lock iff all the children have done.
    if (in->num_pending_children == 0) {
        pthread_mutex_unlock(&(in->mutex));
    }
    return NULL;

  error:
    return NULL;
}

static void    *
thread_routine_mem(void *info)
{
    struct info    *in,
                    ch;
    int             m,
                    index,
                    left_size,
                    right_size;
    int             status;
    pthread_t       thread;

    check(info != NULL, "Invalid argument.");
    in = (struct info *) info;
    check_arguments(in->A, in->n, in->p);

    if (in->p == 1 || in->n <= THRESHOLD) {
        quickSort(in->A, in->n);
    } else {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;

        check_partition(in->n, m, left_size, right_size);

        ch.A = &(in->A[index]);
        ch.n = right_size;
        ch.p = in->p / 2;
        ch.num_pending_children = 0;
        ch.done = 0;

        status = pthread_create(&thread, NULL, thread_routine_mem, &ch);
        check(status == 0, "Cannot create thread");

        in->n = left_size;
        in->p /= 2;

        ++(in->num_pending_children);
        thread_routine_mutex(in);
        --(in->num_pending_children);

        while (ch.done != 1) {
            // Spin;
        }
    }

    if (in->num_pending_children == 0) {
        in->done = 1;
    }

    return NULL;

  error:
    return NULL;
}


// concurrent quick sort using pthreads
void
quickThread(int *pA, int pn, int p, enum WaitMechanismType pWaitMech)
{
    pthread_t       root;
    void           *res;
    struct info     r = {
        .A = pA,
        .n = pn,
        .p = p,
        .num_pending_children = 0,
        .done = 0
    };
    int             status;

    check_arguments(pA, pn, p);

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    switch (pWaitMech) {

    case WAIT_JOIN:
        status = pthread_create(&root, NULL, thread_routine_join, &r);
        check(status == 0, "Cannot create the root thread.");
        pthread_join(root, &res);
        break;

    case WAIT_MUTEX:
        status = pthread_mutex_init(&(r.mutex), NULL);
        check(status == 0, "Canot create the mutex for the root");
        status = pthread_mutex_lock(&(r.mutex));
        check(status == 0, "Cannot lock");
        status = pthread_create(&root, NULL, thread_routine_mutex, &r);
        check(status == 0, "Cannot create the root thread.");
        status = pthread_mutex_lock(&(r.mutex));
        check(status == 0, "Cannot lock");
        pthread_mutex_destroy(&(r.mutex));
        break;

    case WAIT_MEMLOC:
        status = pthread_create(&root, NULL, thread_routine_mem, &r);
        check(status == 0, "Cannot create the root thread.");
        while (r.done != 1) {
            // Spin;
        }
        break;

    default:
        log_warn("I don't understand");
        goto error;
    }

    return;

  error:
    return;
}
