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
static inline void debug_printArray(int A[], int n);

static ssize_t  read_all_ints(int fildes, int *buf, int ntimes);

static ssize_t  write_all_ints(int fildes, int *buf, int ntimes);

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
                    i,
                    tag,
                    last_tag_used,
                    new_tag,
                    m,
                    num_children_created,
                    child_id,
                    child_tag;

    int             fdmax;
    fd_set          read_fds;


    int             k = lg2(p);

    // File descriptors
    int             fd_cp[k][2],
                    fd_reply;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;

    int             child_id_to_tag[k],
                    offset_table[k],
                    size_table[k];

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    fd_reply = -1;
    for (i = 0; i < k; ++i) {
        fd_cp[i][0] = -1;
        fd_cp[i][1] = -1;
    }

    child_id = 0;
    tag = 0;
    last_tag_used = p + 1;

    log_info("Input:");
    debug_printArray(A, n);

    while (p != 1) {
        new_tag = (tag + last_tag_used) / 2;
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;

        // Do not be absurd.
        if (left_size <= 0 || right_size <= 0) {
            break;
        }

        check(pipe(fd_cp[child_id]) == 0, "%d:\tCannot pipe().", tag);

        log_info("%d:\tPrepare to create %d", tag, new_tag);

        switch (fork()) {
        case -1:
            log_warn("Cannot fork()");
            goto error;
        case 0:
            // Information
            tag = new_tag;

            A = &A[index];
            n = right_size;

            fd_reply = dup(fd_cp[child_id][1]);

            for (i = 0; i < k; ++i) {
                CLOSEFD(fd_cp[i][1]);
                CLOSEFD(fd_cp[i][0]);
            }

            child_id = 0;
            log_info("%d:\tIs created.", new_tag);
            debug_printArray(A, n);
            break;

        default:
            // Parent does this.
            log_info("%d:\tis left with %d elements:", tag, left_size);

            size_table[child_id] = right_size;
            offset_table[child_id] = index;
            child_id_to_tag[child_id] = new_tag;
            last_tag_used = new_tag;

            n = left_size;

            CLOSEFD(fd_cp[child_id][1]);
            ++child_id;
            debug_printArray(A, n);
            break;
        }
        p /= 2;
    }

    quickSort(A, n);

    log_info("%d\tHas done:", tag);
    debug_printArray(A, n);

    if (tag % 2 == 0) {
        num_children_created = child_id;
        while (num_children_created > 0) {
            fdmax = -1;
            FD_ZERO(&read_fds);
            for (i = 0; i < child_id; ++i) {
                int             fd;
                fd = fd_cp[i][0];
                if (fd < 0) {
                    continue;
                }
                FD_SET(fd, &read_fds);
                fdmax = max(fdmax, fd);
            }

            check(fdmax != -1, "%d:\tThe file descriptors are messed",
                  tag);
            ++fdmax;

            select(fdmax, &read_fds, NULL, NULL, NULL);

            log_info("%d:\tis unblockded", tag);
            for (i = 0; i < child_id; ++i) {
                int             fd = fd_cp[i][0];
                if (fd == -1 || fd >= fdmax) {
                    continue;
                }
                if (FD_ISSET(fd, &read_fds)) {
                    child_tag = child_id_to_tag[i];
                    log_info
                        ("%d:\tReading %d elements, offset %d, from: %d",
                         tag, size_table[i], offset_table[i], i);
                    num_ints_read =
                        read_all_ints(fd,
                                      A + offset_table[i], size_table[i]);
                    check(num_ints_read != -1, "Panic %d, %d %d", tag,
                          child_tag, fd_cp[i][0]);
                    check(num_ints_read == size_table[i],
                          "In %d %d Cannot read from %d. Is %ld, should be %d",
                          tag, i, child_tag, num_ints_read, size_table[i]);
                    n += num_ints_read;
                    log_info
                        ("%d\tHas read %ld elements from %d at offset %d:",
                         tag, num_ints_read, i, offset_table[i]);
                    debug_printArray(A + offset_table[i], size_table[i]);
                    CLOSEFD(fd_cp[i][0]);
                    --num_children_created;
                }
            }
        }
    }

    if (tag != 0) {
        log_info("%d:\tSending %d elements to its parent", tag, n);
        num_ints_written = write_all_ints(fd_reply, A, n);
        check(num_ints_written == n, "Cannot write to %d", fd_reply);
        log_info("%d:\tSent %d elements to its parent", tag,
                 (int) num_ints_written);
    }

    while (child_id != 0) {
        log_info("%d:\tWaiting, still has %d children", tag, child_id);
        wait(NULL);
        --child_id;
    }

    if (tag == 0) {
        debug_printArray(A, n);
        return;
    } else {
        _exit(EXIT_SUCCESS);
    }

  error:
    for (i = 0; i < k; ++i) {
        CLOSEFD(fd_cp[i][0]);
        CLOSEFD(fd_cp[i][1]);
    }

    if (tag == 0) {
        return;
    } else {
        _exit(EXIT_FAILURE);
    }
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
                    i,
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
    setbuf(stderr, NULL);

    int             k = lg2(p);

    int             child_id_to_tag[k];

    // File descriptors
    int             fd_listener,
                    fd_reads[k],
                    fd_p,
                    fd_c,
                    optval,
                    fd_reply;

    struct addrinfo hints,
                   *servinfo,
                   *ptr;
    struct sockaddr_storage info;
    socklen_t       info_len;

    // Status code
    ssize_t         num_ints_read,
                    num_ints_written;

    int             offset_table[k],
                    size_table[k];

    char            port_buf[16];       // must write port number into a

    child_id = 0;

    tag = 0;
    last_tag_used = p + 1;

    log_info("Input:");
    debug_printArray(A, n);

    while (p != 1) {
        // Be practical!
        // if (size <= 10) {
        // break;
        // }
        new_tag = (tag + last_tag_used) / 2;
        m = partition(A, n);
        index = m + 1;
        left_size = m + 1;
        right_size = n - m - 1;
        // Do not be absurd.
        if (left_size <= 0 || right_size <= 0) {
            break;
        }

        if (child_id == 0) {
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
                if (setsockopt
                    (fd_listener, SOL_SOCKET, SO_REUSEADDR, &optval,
                     sizeof(optval)) == -1) {
                    continue;
                }
                if (bind
                    (fd_listener, servinfo->ai_addr,
                     servinfo->ai_addrlen) == -1) {
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
            log_info("%d:\tListening at %s", tag, port_buf);
        }

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

            A = &A[index];
            n = right_size;

            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            log_info("Connection to %s", port_buf);
            check(getaddrinfo("localhost", port_buf, &hints, &servinfo) ==
                  0, "cannot getaddrinfo");
            optval = 1;
            for (ptr = servinfo; ptr != NULL; ptr = ptr->ai_next) {
                log_info("Connection to %s", port_buf);
                fd_c =
                    socket(ptr->ai_family, ptr->ai_socktype,
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

            fd_reply = dup(fd_c);

            debug_printArray(A, n);

            CLOSEFD(fd_c);
            CLOSEFD(fd_p);
            CLOSEFD(fd_listener);
            for (i = 0; i < child_id; ++i) {
                CLOSEFD(fd_reads[i]);
            }

            child_id = 0;
            break;

        default:
            // Parent does this.

            size_table[child_id] = right_size;
            offset_table[child_id] = index;
            child_id_to_tag[child_id] = new_tag;
            last_tag_used = new_tag;

            fd_p = accept(fd_listener, NULL, NULL);
            check(fd_p != -1, "%d cannot accept()", tag);

            fd_reads[child_id] = dup(fd_p);
            ++child_id;
            CLOSEFD(fd_p);

            n = left_size;

            log_info("%d:\tis left with %d elements:", tag, left_size);
            debug_printArray(A, n);
            break;
        }
        p /= 2;
    }

    quickSort(A, n);

    log_info("%d\tHas done:", tag);
    debug_printArray(A, n);

    if (tag % 2 == 0) {
        num_children_created = child_id;
        while (num_children_created > 0) {
            fdmax = -1;
            FD_ZERO(&read_fds);
            for (i = 0; i < child_id; ++i) {
                int             fd;
                fd = fd_reads[i];
                if (fd == -1) {
                    continue;
                }
                FD_SET(fd, &read_fds);
                fdmax = max(fdmax, fd);
            }
            check(fdmax != -1, "%d:\tThe file descriptors are messed",
                  tag);
            ++fdmax;

            select(fdmax, &read_fds, NULL, NULL, NULL);

            log_info("%d:\tis unblockded", tag);
            for (i = 0; i < child_id; ++i) {
                int             fd = fd_reads[i];
                if (fd == -1 || fd >= fdmax) {
                    continue;
                }
                if (FD_ISSET(fd, &read_fds)) {
                    child_tag = child_id_to_tag[i];
                    log_info
                        ("%d:\tReading %d elements, offset %d, from: %d",
                         tag, size_table[i], offset_table[i], i);
                    num_ints_read = read_all_ints(fd,
                                                  A + offset_table[i],
                                                  size_table[i]);
                    check(num_ints_read == size_table[i],
                          "In %d %d Cannot read from %d. Is %ld, should be %d",
                          tag, i, child_tag, num_ints_read, size_table[i]);
                    n += num_ints_read;
                    log_info
                        ("%d\tHas read %ld elements from %d at offset %d:",
                         tag, num_ints_read, i, offset_table[i]);
                    debug_printArray(A + offset_table[i], size_table[i]);
                    CLOSEFD(fd_reads[i]);
                    --num_children_created;
                }
            }
        }
    }

    if (tag != 0) {
        log_info("%d:\tSending %d elements to its parent", tag, n);
        num_ints_written = write_all_ints(fd_reply, A, n);
        check(num_ints_written == n, "%d:\tCannot write to %d", tag,
              fd_reply);
        log_info("%d:\tSent %d elements to its parent", tag,
                 (int) num_ints_written);
    }

    while (child_id != 0) {
        wait(NULL);
        --child_id;
    }

    if (tag == 0) {
        debug_printArray(A, n);
        return;
    } else {
        _exit(EXIT_SUCCESS);
    }


  error:
    if (tag == 0) {
        return;
    } else {
        _exit(EXIT_FAILURE);
    }
}

struct info {
    int tag;
    int *A;
    int n;
    int p;
    int last_tag_used;
    int child_id;
    enum WaitMechanismType pWaitMech;
    pthread_t *children;
};

static void *
thread_routine(void * info)
{
    check(info != NULL, "Invalid argument.");
    struct info *in = (struct info *)info;
    int k,
        m,
        index,
        left_size,
        right_size,
        thread_created,
        new_tag;
    void *res;
    struct info *ch;

    k = lg2(in->p);

    log_info("%d is initialised.", in->tag);

    debug_printArray(in->A, in->n);
    in->children = malloc(k * sizeof(pthread_t));
    check_mem(in->children);
    in->child_id = 0;
    while (in->p != 1) {
        m = partition(in->A, in->n);
        index = m + 1;
        left_size = m + 1;
        right_size = in->n - m - 1;
        new_tag = (in->last_tag_used + in->tag) / 2;
        ch = malloc(sizeof(struct info));
        memset(ch, 0, sizeof(struct info));
        ch->tag = new_tag;
        ch->A = &(in->A[index]);
        ch->n = right_size;
        ch->p = in->p / 2;
        ch->last_tag_used = in->last_tag_used;
        ch->pWaitMech = in->pWaitMech;
        check(pthread_create(&(in->children[in->child_id]), NULL,
                               thread_routine, ch) == 0,
                             "%d:\tCannot create a new thread", in->tag);
        // in.tag;
        // in.A;
        in->n = left_size;
        in->p /= 2;
        in->last_tag_used = new_tag;
        in->child_id += 1;
    }

    quickSort(in->A, in->n);

    switch (in->pWaitMech) {
    case WAIT_JOIN:
        for (thread_created = in->child_id - 1; thread_created > 0; --thread_created) {
            check(pthread_join(in->children[thread_created], &res) == 0,
                  "%d:\tCannot pthread_join. %d", in->tag, thread_created);
            // free(in->children[thread_created]);
        }
        break;
    case WAIT_MUTEX:
        // TODO
        break;
    case WAIT_MEMLOC:
        // TODO
        break;
    }

    return NULL;

error:
    return NULL;
}


// concurrent quick sort using pthreads
void
quickThread(int *pA, int pn, int p, enum WaitMechanismType pWaitMech)
{
    int k;
    k = lg2(p);
    pthread_t root;
    struct info r;
    void *res;
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    r.tag = 0;
    r.A = pA;
    r.child_id = 0;
    r.last_tag_used = p + 1;
    r.n = pn;
    r.p = p;
    r.pWaitMech = pWaitMech;
    log_info("%d", (&r)->n);
    check(pthread_create(&root, NULL, thread_routine, &r) == 0,
          "Cannot create the root thread.");
    log_info("The root is created");
    pthread_join(root, &res);

error:
    return;
}