#define _POSIX_C_SOURCE 200112L // struct addrinfo
#define FUSE_USE_VERSION 26
#define _XOPEN_SOURCE 500

#include "rlfs.h"
#include "rlfsop.hpp"
#include "util.hpp"

extern rlfsconfig config;
extern struct fuse_operations rlfs_operations;

/*
 * Retrieve RLFS specific options
 *
 * Notes:
 *     For now, several specific options are provided:
 *     --node NODE: The node configuration file
 *     --chunksize VALUE: The chunk size in bytes when transfer data.
 *     Any other options will be passed to FUSE library.
 *
 * Return:
 *     If successful, return 0, otherwise -1.
 */

int parse_cmd_args(int* argc, char* argv[]) {
    unsigned maxmsglen = 4096;
    char helpmsg[maxmsglen + 1];
    memset(helpmsg, 0, maxmsglen + 1);
    snprintf(helpmsg, maxmsglen,
            "Usage: %s <RLFS options> [FUSE options]\n"
            "\n"
            "General Options:\n"
            "\t -o opt, [opt...] mount options\n"
            "\t [-h|--help] print help\n"
            "\t [-v|--version] print fuse version\n"
            "\n"
            "RLFS Options:\n"
            "\t --node NODE The node configuration file\n"
            "\t --mount DIR The directory to mount on\n"
            "\t [--thread VALUE] The number of thread. Should be in [1, %d], and the default value is %d\n"
            "\nFor example:\n"
            "\n"
            "\t %s --node ~/nodeconfig --mount ~/point -f\n",
            argv[0], MAX_THREAD_NUM, MAX_THREAD_NUM / 2, argv[0]);
    helpmsg[maxmsglen] = '\0';

    if(*argc == 1) {
        printf("%s", helpmsg);
        return -1;
    }

    for(int i = 1; i < *argc; ++i) {
        logmsg(0, FLOC"argv[%d] ==> [%s]\n", LOC, i, argv[i]);
        if(strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("%s", helpmsg);
            return -1;
        } else if(strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--version") == 0) {
            printf("RLFS version %s\n", PACKAGE_VERSION);
            return -1;
        } else if(strcmp(argv[i], "--node") == 0) {
            argv[i] = "";
            ++i;
            if(i < *argc && argv[i][0] != '-') {
                char *abspath = realpath(argv[i], NULL);
                memcpy(config.nodeconf, abspath, PATH_MAX);
                free(abspath);
                argv[i] = "";
            }
            else {
                logmsg(stderr, FLOC"Use %s -h to get help.\n", LOC, argv[0]);
                return -1;
            }
        } else if(strcmp(argv[i], "--mount") == 0) {
            argv[i] = "";
            ++i;
            if(i < *argc && argv[i][0] != '-') {
                char abspath[PATH_MAX] = {0};
                realpath(argv[i], abspath);
                logmsg(0, FLOC"argv[%d] = %s realpath = %s\n",
                        LOC, i, argv[i], abspath);
                if(strlen(abspath) == 0) {
                    logmsg(stderr,
                            FLOC"If the mount directory exists, umount it first!\n", LOC);
                    return -1;
                }
                memcpy(config.root, abspath, PATH_MAX);
            }
        } else if(strcmp(argv[i], "--thread") == 0) {
             argv[i] = "";
             ++i;
             if(i < *argc && argv[i][0] != '-') {
                 unsigned thread = atoi(argv[i]);
                 if(thread > 0 && thread <= MAX_THREAD_NUM) {
                     config.thread = thread;
                 } else {
                     logmsg(stderr, FLOC"The number of thread should be in [1, %d]", LOC, MAX_THREAD_NUM);
                 }
                 argv[i] = "";
             }
        }
    }

    if('\0' == config.nodeconf[0]) {
        logmsg(stderr,
                "Hey, guys! I am hungry .. "
                "please give me some nodes <--node NODE> to eat ...\n");
        return -1;
    }

    shrink(argc, argv, strlen);

    return 0;
}

int main(int argc, char *argv[]) {
    initconfig();
    if(parse_cmd_args(&argc, argv) != 0) return -1;

    if(read_node_conf(config.nodeconf) == 0) {
        printf("The number of nodes are : %d\n", config.nodenum);
        printf("The number of threds are : %d\n", config.thread);
        printf("The META node is: %s:%s\n", config.nodejar[config.metanodeid].host, config.nodejar[config.metanodeid].port);
        printf("The rlfs local storage path: %s\n", config.rlfstmp);
        printf("The rlfs mouted path: %s\n", config.root);
    } else {
        printf("Node loading is falied!\n");
        return -1;
    }

    struct bb_state *bb_data = malloc(sizeof(struct bb_state));
    if (bb_data == NULL) { perror("main calloc"); abort(); }
    bb_data->rootdir = config.rlfstmp;
    bb_data->logfile = log_open();

    int fuse_stat = fuse_main(argc, argv, &rlfs_operations, bb_data);
    free(bb_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);
    return fuse_stat;
}
