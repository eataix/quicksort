#ifndef __UTILS_H
#define __UTILS_H

#define ARRAY_SIZE(x) (sizeof((x)) / sizeof((x)[0]))

#define CLOSEFD(fd) do {                                                      \
        if (fd != -1) {                                                       \
            close(fd);                                                        \
            fd = -1;                                                          \
        }                                                                     \
} while (0)

#endif
