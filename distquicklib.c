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
static ssize_t  read_all_ints(int fildes, int *buf, int ntimes);

static ssize_t  write_all_ints(int fildes, int *buf, int ntimes);

static inline void debug_printArray(int A[], int n);

static inline in_port_t get_in_port(struct sockaddr *sa);

/**
 * An interface to the printArray() function.
 * Very useful while debugging.
 **/
static inline void
debug_printArray(int A[], int n)
{
#ifdef DEBUG
    printArray(A, n);
#endif
}


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


/**
 * Ditto
 **/
static          ssize_t
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
        check(bytes_written > 0, "Cannot write %d", fildes);
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
                    m;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;
    int             fd_cp[2];

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    log_info("Input:");
    debug_printArray(A, n);

    if (p == 1) {
        quickSort(A, n);
    } else {
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;

        check(pipe(fd_cp) == 0, "Cannot pipe().");

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;
        case 0:
            quickPipe(&A[index], right_size, p / 2);
            num_ints_written =
                write_all_ints(fd_cp[1], &A[index], right_size);
            check(num_ints_written == right_size, "Cannot write.");
            _exit(0);
            break;
        default:
            quickPipe(A, left_size, p / 2);
            num_ints_read = read_all_ints(fd_cp[0], &A[index], right_size);
            check(num_ints_read == right_size, "Cannot read.");
            wait(NULL);
            break;
        }
    }

    return;

  error:
    return;
}

// distributed quick sort using sockets
void
quickSocket(int A[], int n, int p)
{
    int             index,
                    left_size,
                    right_size,
                    m;

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    // File descriptors
    int             fd_listener,
                    fd_p,
                    fd_c;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;

    struct sockaddr_in server;
    socklen_t       namelen;

    namelen = sizeof(server);

    if (p == 1) {
        quickSort(A, n);
    } else {
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;

        fd_listener = socket(AF_INET, SOCK_STREAM, 0);
        check(fd_listener != -1, "no socket");

        server.sin_family = AF_INET;
        server.sin_port = 0;
        server.sin_addr.s_addr = INADDR_ANY;

        check(bind
              (fd_listener, (struct sockaddr *) &server, sizeof(server))
              != -1, "Cannot bind(2).");
        check(getsockname
              (fd_listener, (struct sockaddr *) &server, &namelen)
              != -1, "Cannot getsockname(2).");
        check(listen(fd_listener, 1) == 0, "Cannot listen(2).");

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;

        case 0:
            close(fd_listener);

            quickSocket(&A[index], right_size, p / 2);
            fd_c = socket(AF_INET, SOCK_STREAM, 0);
            check(fd_c != -1, "Cannot create a socket");
            check(connect
                  (fd_c, (struct sockaddr *) &server, sizeof(server))
                  != -1, "Cannot connect");
            num_ints_written = write_all_ints(fd_c, &A[index], right_size);
            check(num_ints_written == right_size, "Cannot write");

            close(fd_c);
            _exit(EXIT_SUCCESS);

        default:
            quickSocket(A, n, p / 2);

            fd_p = accept(fd_listener, NULL, NULL);
            num_ints_read = read_all_ints(fd_p, &A[index], right_size);
            check(num_ints_read == right_size, "Cannot read");

            close(fd_listener);
            close(fd_p);
            return;
        }
    }

  error:
    return;
}

struct info {
    int            *A;
    int             n;
    int             p;
    int             i;
    int             num_pending_children;
    int             done;
    pthread_cond_t  doneCond;
    pthread_mutex_t mutex;
};

static void    *
thread_routine_join(void *info)
{
    check(info != NULL, "Invalid argument.");
    struct info    *in = (struct info *) info;
    int             m,
                    index,
                    left_size,
                    right_size;

    void           *res;
    struct info     ch;
    pthread_t       thread;
    log_info("A thread is created");

    if (in->p == 1) {
        quickSort(in->A, in->n);
    } else {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;
        ch.A = &(in->A[index]);
        ch.n = right_size;
        ch.p = in->p / 2;
        check(pthread_create(&thread, NULL, thread_routine_join, &ch) == 0,
              "Cannot create thread");
        in->n = left_size;
        in->p /= 2;
        thread_routine_join(in);
        pthread_join(thread, &res);
    }
    return NULL;
  error:
    return NULL;
}

static void    *
thread_routine_mutex(void *info)
{
    check(info != NULL, "Invalid argument.");
    struct info    *in = (struct info *) info;
    int             m,
                    index,
                    left_size,
                    right_size;

    struct info     ch;
    pthread_t       thread;

    // log_warn("A thread is created");
    if (in->p == 1) {
        quickSort(in->A, in->n);
        log_info("Sorted");
    } else {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;

        ch.A = &(in->A[index]);
        ch.n = right_size;
        ch.p = in->p / 2;
        ch.num_pending_children = 0;
        check(pthread_mutex_init(&(ch.mutex), NULL) == 0,
              "Cannot initialise a mutex");
        check(pthread_mutex_lock(&(ch.mutex)) == 0,
              "Cannot acquire the lock");
        check(pthread_create(&thread, NULL, thread_routine_mutex, &ch) ==
              0, "Cannot create thread");

        in->n = left_size;
        in->p /= 2;
        ++(in->num_pending_children);
        thread_routine_mutex(in);
        --(in->num_pending_children);
        check(pthread_mutex_lock(&(ch.mutex)) == 0, "Cannot wait");
        pthread_mutex_destroy(&(ch.mutex));
    }

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
    check(info != NULL, "Invalid argument.");
    struct info    *in = (struct info *) info;
    int             m,
                    index,
                    left_size,
                    right_size;

    struct info     ch;
    pthread_t       thread;

    if (in->p == 1) {
        quickSort(in->A, in->n);
    } else {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;

        ch.A = &(in->A[index]);
        ch.n = right_size;
        ch.p = in->p / 2;
        ch.num_pending_children = 0;
        ch.done = 0;
        check(pthread_mutex_init(&(ch.mutex), NULL) == 0,
              "Cannot initialise a mutex");
        check(pthread_cond_init(&(ch.doneCond), NULL) == 0,
              "Cannot create a condition.");

        check(pthread_create(&thread, NULL, thread_routine_mem, &ch) ==
              0, "Cannot create thread");

        in->n = left_size;
        in->p /= 2;

        ++(in->num_pending_children);
        thread_routine_mutex(in);
        --(in->num_pending_children);

        // You can do a busy waiting if you like.
        // Yet, I will not do it for the shake of performance.
        //
        // while (ch.done != 1) {
        // // Spin;
        // }
        check(pthread_mutex_lock(&(ch.mutex)) == 0, "Cannot lock");
        while (ch.done != 1) {
            check(pthread_cond_wait(&(ch.doneCond), &(ch.mutex)) == 0,
                  "Cannot");
        }
        check(pthread_mutex_unlock(&(ch.mutex)) == 0, "Cannot unlock");
        pthread_cond_destroy(&(ch.doneCond));
        pthread_mutex_destroy(&(ch.mutex));
    }



    if (in->num_pending_children == 0) {
        check(pthread_mutex_lock(&(in->mutex)) == 0, "Cannot lock");
        in->done = 1;
        check(pthread_mutex_unlock(&(in->mutex)) == 0, "Cannot unlock");
        check(pthread_cond_broadcast(&(in->doneCond)) == 0,
              "Cannot boradcast");
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
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    struct info     r = {
        .A = pA,
        .n = pn,
        .p = p,
    };

    switch (pWaitMech) {

    case WAIT_JOIN:
        check(pthread_create(&root, NULL, thread_routine_join, &r) == 0,
              "Cannot create the root thread.");
        pthread_join(root, &res);
        break;

    case WAIT_MUTEX:
        r.num_pending_children = 0;
        check(pthread_mutex_init(&(r.mutex), NULL) == 0,
              "Canot create the mutex for the root");
        check(pthread_mutex_lock(&(r.mutex)) == 0, "Cannot lock");
        check(pthread_create(&root, NULL, thread_routine_mutex, &r) == 0,
              "Cannot create the root thread.");
        check(pthread_mutex_lock(&(r.mutex)) == 0, "Cannot lock");
        pthread_mutex_destroy(&(r.mutex));
        break;

    case WAIT_MEMLOC:
        r.num_pending_children = 0;
        check(pthread_mutex_init(&(r.mutex), NULL) == 0,
              "Cannot initialise a mutex");
        check(pthread_cond_init(&(r.doneCond), NULL) == 0,
              "Cannot create a condition.");
        check(pthread_create(&root, NULL, thread_routine_mem, &r) == 0,
              "Cannot create the root thread.");
        check(pthread_mutex_lock(&(r.mutex)) == 0, "locks");
        while (r.done != 1) {
            check(pthread_cond_wait(&(r.doneCond), &(r.mutex)) == 0,
                  "Cannot");
        }
        check(pthread_mutex_unlock(&(r.mutex)) == 0, "unlocks");
        pthread_cond_destroy(&(r.doneCond));
        pthread_mutex_destroy(&(r.mutex));
        break;

    }
    return;

  error:
    return;
}
