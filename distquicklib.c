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

static inline void
debug_printArray(int A[], int n)
{
#ifdef DEBUG
    printArray(A, n);
#endif
    return;
}


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

static inline   in_port_t
get_in_port(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return (((struct sockaddr_in *) sa)->sin_port);
    }

    return (((struct sockaddr_in6 *) sa)->sin6_port);
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
                    fd_c,
                    optval;

    struct addrinfo hints,
                   *servinfo,
                   *ptr;
    struct sockaddr_storage info;
    socklen_t       info_len;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;

    char            port_buf[16];       // must write port number into a

    if (p == 1) {
        quickSort(A, n);
    } else {
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;
        check(getaddrinfo("localhost", NULL, &hints, &servinfo) == 0,
              "cannot getaddrinfo");

        optval = 1;
        for (ptr = servinfo; ptr != NULL; ptr = ptr->ai_next) {
            fd_listener = socket(ptr->ai_family, ptr->ai_socktype,
                                 ptr->ai_protocol);
            if (fd_listener == -1) {
                continue;
            }
            if (setsockopt(fd_listener, SOL_SOCKET, SO_REUSEADDR, &optval,
                           sizeof(optval)) == -1) {
                continue;
            }
            if (bind(fd_listener, servinfo->ai_addr, servinfo->ai_addrlen)
                == -1) {
                close(fd_listener);
                continue;
            }
            if (listen(fd_listener, 2) == -1) {
                close(fd_listener);
                continue;
            }
            break;
        }
        check(ptr != NULL, "Failed to bind\n");
        info_len = sizeof info;
        getsockname(fd_listener, (struct sockaddr *) &info, &info_len);
        sprintf(port_buf, "%d",
                ntohs(get_in_port((struct sockaddr *) &info)));

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;
            // break;
        case 0:
            quickSocket(&A[index], right_size, p / 2);
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            check(getaddrinfo("localhost", port_buf, &hints, &servinfo)
                  == 0, "cannot getaddrinfo");
            optval = 1;
            for (ptr = servinfo; ptr != NULL; ptr = ptr->ai_next) {
                log_info("Connection to %s", port_buf);
                fd_c = socket(ptr->ai_family, ptr->ai_socktype,
                              ptr->ai_protocol);
                if (fd_c == -1) {
                    continue;
                }
                if (connect(fd_c, ptr->ai_addr, ptr->ai_addrlen) == -1) {
                    CLOSEFD(fd_c);
                    continue;
                }
                break;
            }
            check(ptr != NULL, "Cannot connect");
            num_ints_written = write_all_ints(fd_c, &A[index], right_size);
            _exit(EXIT_SUCCESS);
        default:
            quickSocket(A, n, p / 2);
            fd_p = accept(fd_listener, NULL, NULL);
            num_ints_read = read_all_ints(fd_p, &A[index], right_size);
            check(num_ints_read == right_size, "Cannot read");
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
        check(pthread_create(&thread, NULL, thread_routine_mem, &ch) ==
              0, "Cannot create thread");

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
        r.done = 0;
        r.num_pending_children = 0;
        check(pthread_create(&root, NULL, thread_routine_mem, &r) == 0,
              "Cannot create the root thread.");
        while (r.done != 1) {
        }
        break;
    }

    return;

  error:
    exit(EXIT_FAILURE);
}
