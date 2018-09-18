#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h> // va_list
#include <limits.h> // INT_MAX

#define MAX_FMT_SIZE  0x256

#ifndef NAME_MAX
#   define NAME_MAX         255
#endif

#ifndef  PATH_MAX
#   define PATH_MAX        4096
#endif

#ifndef BUGNOFREE_UTIL_HPP
#define BUGNOFREE_UTIL_HPP

// #define FIREBUG

int get_suffix(char *path, char suffix[NAME_MAX]);
int get_parentdir(char *path, char parentdir[PATH_MAX]);
int get_fullname(char *path, char fullname[NAME_MAX]);
int get_barename(char *path, char barename[NAME_MAX]);

#define BUG(msg...) (printf("\n==> <%s>.<%s>#%d <==\n[DBG] %s\n", __FILE__, __func__, __LINE__, ##msg))

/*
 * Print log messages
 *
 * @param stream: A FILE descriptor. For stdin(0), it outputs debug information
 * only when the macro FIREBUG is defined. For stdout(1), stderr(2) and others,
 * they keep what they are.
 *
 * Notes:
 *     Two log macros are provided to assist you print the precise location:
 *     You may use
 *
 *     logmsg(stdout, FLOC"I am from %s.", LOC, "Chinese")
 *
 * which produces:
 *
 *     [+] <Tue Aug 28 09:36:01 2018>|</path/to/src/program.c>.<func>#lineno
 *     I am from China.
 */
#define FLOC "<%s>.<%s>#%d\n"
#define LOC __FILE__, __func__, __LINE__
void logmsg(FILE * stream, const char * format, ...) {
    int firebug = 0;

#ifdef FIREBUG
    firebug = 1;
#endif
    // Suppress the debug output
    if(stream == 0 && firebug == 0) return;

    int fmtlen = 0x100;
    char* xformat = (char*) malloc(fmtlen + 1);
    memset(xformat, 0, fmtlen + 1);

    if(stream == 0) {
        char dbgtag[] = "[!] FIREBUG |";
        strncpy(xformat, dbgtag, fmtlen);
        stream = stdout;
    } else {
        time_t rawtime;
        struct tm * timeinfo;
        time (&rawtime);
        timeinfo = localtime(&rawtime);
        // From the manual here: http://www.cplusplus.com/reference/ctime/asctime/,
        // we know that the string is followed by a new-line character ('\n') and terminated with
        // a null-character. I really do not know why the library is implemented
        // like that. Really a fuckkkkking thing....
        char *nowtm = asctime(timeinfo);
        // Let's remove the '\n' character Notice that the index is not
        // strlen(nowtm) - 2
        nowtm[strlen(nowtm) - 1] = '\0';
        snprintf(xformat, fmtlen, "[+] <%s>|", nowtm);
        xformat[fmtlen] = '\0';
    }

    va_list args;
    va_start(args, format);
    strncat(xformat, format, fmtlen - strlen(xformat));
    vfprintf(stream, xformat, args);
    va_end(args);
    free(xformat);
}

/*
 * Removing spaces in both sides of a string
 *
 * @param str: The string to remove spaces from.
 * @param stripped: Location where the resulted string is stored into.
 */
void str_strip(char* str, char* stripped) {
    int len = strlen(str);
    if(len == 0) {
        stripped[0] = '\0';
    } else {
        char *p = str, *q = str + len - 1;
        // There is a total of four situations(where '.' is used to represent a
        // space):
        // 1: "..ab.."
        // 2: "..ab"
        // 3: "ab.."
        // 4: "...."
        while(p <= q && isspace(*p)) ++p;
        while(q > p && isspace(*q)) --q;
        int i = 0;
        while(p <= q) stripped[i++] = *p++;
        stripped[i] = '\0';
    }
}


/*
 * Shrink a C string array in place by removing all empty elements.
 *
 * @param argc: The length of the array
 * @param argv: The C string array
 * @param check: A filter function. The function should return 0 if the checked
 * element is to be removed.
 *
 * Requirements:
 *
 *     #include <stdio.h>
 *     #include <string.h>
 *
 * Example:
 *
 *     The following codes will remove any elements whose length is zero:
 *
 *         char* ss[] = { "123", "1234567", "", "", "", "d", "e" };
 *         int cnt = 7;
 *         shrink(&cnt, ss, strlen);
 *
 *     and the cnt will become 4 when the function is executed.
 */

void shrink(int *argc, char* argv[], unsigned long (*check)(const char*)) {
    char **p = argv;
    while(p < argv + (*argc)) {
        if(check(*p) == 0) break;
        else ++p;
    }
#ifdef DBG
    printf("hole = %ld\n", p - argv);
#endif
    char **q = p + 1;
    while(q < argv + (*argc)) {
#ifdef DBG
        printf("curstr: %s\n", *q);
#endif
        if(check(*q) != 0) {
            *p = *q;
            ++p;
        }
        ++q;
    }
    // The pointer p always points to the right of the last valid element,
    // hence, the array's length is p - argv
    *argc = p - argv;
}


/*
 * A simple C memory manager
 */

typedef struct memnode {
    struct memnode *next;
    unsigned sz;
    void *mem;
    void (*free)(void *);
} memnode;

typedef struct {
    memnode *head;
    memnode *tail;
} memmgr;

memmgr *new_memmgr() {
    memmgr *mmgr = (memmgr *)malloc(sizeof(memmgr));

    memnode *mnode = (memnode *)malloc(sizeof(memnode));
    mnode->next = NULL;
    mnode->sz = -1;
    mnode->free = free;
    mnode->mem = mmgr;

    mmgr->head = mnode;
    mmgr->tail = mnode;
    return mmgr;
}

memnode *newnode(memmgr *mmgr, unsigned sz) {
    void *mem = malloc(sz);
    memnode *mnode = (memnode *)malloc(sz);
    mnode->next = NULL;
    mnode->sz = sz;
    mnode->mem = mem;
    mnode->free = free;
    mmgr->tail->next = mnode;
    return mem;
}

void *newmem(memmgr *mmgr, unsigned sz) {
    memnode *mnode = newnode(mmgr, sz);
    return mnode->mem;
}

void *newmemx(memmgr *mmgr, unsigned sz, void (*xfree)(void *)) {
    memnode *mnode = newnode(mmgr, sz);
    mnode->free = xfree;
    return mnode->mem;
}

void free_memmgr(memmgr *mmgr) {
    memnode *mnode = mmgr->head->next;
    while(mnode) {
        (*mnode->free)(mnode->mem);
        memnode *next = mnode->next;
        free(mnode);
        mnode = next;
    }
    (*mmgr->head->free)(mmgr);
}

/*
 * Get the number of digits of an integer (do not include the sign)
 *
 */

unsigned numlen(int x) {
    if(x == 0) return 1;
    int sign = x < 0 ? -1 : 1;
    if(x * 1.0 * sign > INT_MAX) return 0;
    x = x * sign;
    return log10(x * 1.0) + 1;
}

/*
 * Return:
 *     If successed, return the lenght of barename.
 *     If failed, return -1.
 */
int get_barename(char *path, char barename[NAME_MAX]) {
    memset(barename, 0, NAME_MAX);
    if(strlen(path) == 0 || strlen(path) > PATH_MAX) return -1;

    char *beg = path;
    char *end = beg + strlen(path);

    // p points to the first valid char(included)
    char *p = end;
    while(--p >= beg) if(*p == '/') break;
    if(p < beg) p = beg;
    else ++p;

    char *q = p - 1;
    // pdot points to the last dot char if a dot exists
    char *pdot = end;
    while(++q < end) if(*q == '.') pdot = q;

    int namelen = pdot - p;
    if(namelen < 1) return 0;
    else if(namelen > NAME_MAX) {
        logmsg(stderr, FLOC"The file name is too long!\n", LOC);
        return -1;
    } else {
        memcpy(barename, p, namelen);
        return namelen;
    }
}

/*
 * Return:
 *     If successed, return the lenght of fullname.
 *     If failed, return -1.
 */
int get_fullname(char *path, char fullname[NAME_MAX]) {
    if(strlen(path) == 0 || strlen(path) > PATH_MAX) return -1;
    memset(fullname, 0, NAME_MAX);
    char barename[NAME_MAX], suffix[NAME_MAX];
    if(get_barename(path, barename) >= 0 && get_suffix(path, suffix) >= 0) {
        snprintf(fullname, NAME_MAX, "%s%s", barename, suffix);
        return strlen(barename) + strlen(suffix);
    }
    else return -1;
}

/*
 * Return:
 *     If successed, return the lenght of parent directory.
 *     If failed, return -1.
 */
int get_parentdir(char *path, char parentdir[PATH_MAX]) {
    if(strlen(path) == 0 || strlen(path) > PATH_MAX) return -1;
    memset(parentdir, 0, PATH_MAX);

    if(path[0] != '/') {
        logmsg(stderr, FLOC"An absolute path is required!\n", LOC);
        return -1;
    }

    char *p = strrchr(path, '/');
    // The p is sured to have one and parendir_len is sured less than PATH_MAX
    int parendir_len = p - path + 1;
    memcpy(parentdir, path, parendir_len);
    return parendir_len;
}

/*
 * Return:
 *     If successed, return the lenght of suffix.
 *     If failed, return -1.
 */
int get_suffix(char *path, char suffix[NAME_MAX]) {
    if(strlen(path) == 0 || strlen(path) > PATH_MAX) return -1;
    memset(suffix, 0, NAME_MAX);

    char *pdot = strrchr(path, '.');
    if(pdot == NULL) return 0;
    else {
        char *pbackslash = strrchr(pdot, '/');
        if(pbackslash != NULL) return 0;
        else {
            int suffixlen = path + strlen(path) - pdot;
            memcpy(suffix, pdot, NAME_MAX);
            return suffixlen;
        }
    }
}
#endif

