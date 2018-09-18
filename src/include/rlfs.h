#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600 //FTW_DEPTH, FTW_MOUNT ...
#endif

#include <string.h>
#include <stdbool.h>
#include <stdio.h> // perror()
#include <stdlib.h> // abort()
#include <stddef.h> // offsetof()
#include <math.h> // log10(), floor(), notice link with '-lm'

#include <dirent.h> // DIR, dirent structure
#include <errno.h> // ENOENT
#include <ftw.h> // nftw()
#include <libgen.h> // char *dirname(char *path); char *basename(char *path);
#include <linux/limits.h> // PATH_MAX
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <pwd.h> // getpwuid()
#include <sys/types.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <unistd.h>

#include <fuse.h>
#include <openssl/md5.h>

#include "util.hpp"


#ifndef RLFS_H
#define RLFS_H

#define FILE_CHUNK (1 << 22)

#define MAX_THREAD_NUM 0xa
#define MAX_HOST_LEN 0x100
#define MAX_PORT_LEN  0x8
#define MAX_NODE_NUM 0x100
#define MAX_LINE_SIZE 0x1000
#define MAX_PACKET_LEN 0x100

#define TRANS_SEND 0
#define TRANS_RECV 1

#define MSG_FILEMETA (1 << 0)
#define MSG_CHUNKDATA (1 << 1)
#define MSG_NULL (1 << 7)

#define ACTION_META_CREATE (1 << 2)
#define ACTION_META_FILL (1 << 3)
#define ACTION_META_FETCH (1 << 4)
#define ACTION_CHUNK_STORE (1 << 5)
#define ACTION_CHUNK_FETCH (1 << 6)
#define ACTION_NULL (1 << 7)

#define ERR_OK (1 << 0)
#define ERR_RMDIR (1 << 1)
#define ERR_NEWDIR (1 << 2)

#define CREATE_FORCE_IFEXIST 1
#define CREATE_IFNEXIST 2

typedef struct {
    char host[MAX_HOST_LEN];
    char port[MAX_PORT_LEN];
    int live;
} rlfsnode;

typedef struct {
    char root[PATH_MAX]; // The mounted absolute directory
    rlfsnode nodejar[MAX_NODE_NUM];  // Keep all nodes
    int nodenum; // The number of node
    unsigned metanodeid; // Meta Node ID
    unsigned thread; // The number of thread
    char nodeconf[PATH_MAX]; // node configuration file
    char rlfstmp[PATH_MAX]; // Local temporary storage
} rlfsconfig;

/*
 * See http://www.catb.org/esr/structure-packing/ for C structure reorganizing
 */
typedef struct {
    char magic[5];
    char type;
    unsigned short action;
    unsigned idlen;
    unsigned dlen;
    // Must appear in the last, see get_serialize_size() function
    unsigned char *id;
    unsigned char *data;
} rlfsmsg;

typedef struct {
    unsigned char *data;
    // The useful data size. Do not confuse with the capacity which is FILE_CHUNK
    unsigned datalen;
} filechunk;

typedef struct chunkmeta {
    unsigned sz;
    unsigned id;
    char host[MAX_HOST_LEN];
    char port[MAX_PORT_LEN];
} chunkmeta;

int next_live_nodeno(unsigned curidx);
int get_serialize_size(rlfsmsg *msg);
unsigned char *serialize_msg(rlfsmsg *msg);
rlfsmsg *deserialize_msg(unsigned char *buf);
rlfsmsg *mallocmsg(unsigned idlen, unsigned dlen);
void freemsg(rlfsmsg *msg);
void printmsg(rlfsmsg *msg);
int sendrlfs(int socketfd, unsigned char* buf, long bufsz);
unsigned char *recvrlfs(int socketfd);
int sendint(int socketfd, int val);
int recvint(int socketfd);
int checkstatus(int socketfd);
int sendfile(int socketfd, const char *fpath);
int recvfile(int socketfd, const char *fpath);
int transfer_data(int socketfd, int type, void *buf, int len);
static int connect_node(rlfsnode* node);
void initconfig();
int nextchunk(FILE *fp, filechunk* fc);
filechunk *malloc_filechunk();
void free_file_chunk(filechunk *fc);
static int rmfiles(const char *pathname, const struct stat *sbuf,
        int type, struct FTW *ftwb);
void *getchunk(void *arg);
void *sendchunk(void *arg);
int fill_chunkmeta_mtsafe(const char *fpath, chunkmeta *cm, unsigned off);
int zerofile(const char *path, long sz);
int createdir(const char *dpath, int way);
int createperf(const char *perfpath, unsigned recordnum, unsigned recordsz);
int write_perf_time_beg(char *perffile, unsigned recordid, long long starttm);
int write_perf_time_end(char *perffile, unsigned recordid, long long endtm);
void get_perfname(char *file_fullname, char *direction, char perffile[PATH_MAX]);
long long gettimems();

rlfsconfig config;

/*
 * Get next live node index(curidx is not included)
 *
 * Notes:
 *   If no live node exists, return -1, or else the next live node index.
 */
int next_live_nodeno(unsigned curidx) {
    unsigned checkidx = (curidx + 1) % config.nodenum;
    unsigned looplimit = 0;
    for(;;) {
        rlfsnode node = config.nodejar[checkidx];
        int wfd = connect_node(&node);
        if(node.live) {
            close(wfd);
            return checkidx;
        }
        checkidx = (checkidx + 1) % config.nodenum;
        ++looplimit;
        if(looplimit == config.nodenum + 1) {
            logmsg(stderr, FLOC"No live node is found!\n", LOC);
            return -1;
        }
    }
}

int get_serialize_size(rlfsmsg *msg) {
    int sz = offsetof(rlfsmsg, id);
    sz += msg->idlen + msg->dlen;
    return sz;
}

/*
 * Notes:
 *   Remember to free after use
 */
unsigned char *serialize_msg(rlfsmsg *msg){
    unsigned char *buf = (unsigned char *)malloc(get_serialize_size(msg));
    memcpy(buf, &msg->magic, sizeof(msg->magic));
    buf[offsetof(rlfsmsg, type)] = msg->type;
    *(unsigned short *)(buf + offsetof(rlfsmsg, action)) = msg->action;
    *(unsigned *)(buf + offsetof(rlfsmsg, idlen)) = msg->idlen;
    *(unsigned *)(buf + offsetof(rlfsmsg, dlen)) = msg->dlen;
    memcpy(buf + offsetof(rlfsmsg, id), msg->id, msg->idlen);
    memcpy(buf + offsetof(rlfsmsg, id) + msg->idlen, msg->data, msg->dlen);
    return buf;
}

/*
 * Notes:
 *   Remember to free after use
 */
rlfsmsg *deserialize_msg(unsigned char *buf) {
    unsigned idlen = *(unsigned *)(buf + offsetof(rlfsmsg, idlen));
    unsigned dlen = *(unsigned *)(buf + offsetof(rlfsmsg, dlen));
    rlfsmsg *msg = mallocmsg(idlen, dlen);
    memcpy(msg->magic, buf, sizeof(msg->magic));
    msg->type = buf[offsetof(rlfsmsg, type)];
    msg->action = *(unsigned short *)(buf + offsetof(rlfsmsg, action));

    msg->id = (unsigned char *)malloc(idlen);
    memcpy(msg->id, buf + offsetof(rlfsmsg, id), msg->idlen);
    msg->data = (unsigned char *)malloc(dlen);
    memcpy(msg->data, buf + offsetof(rlfsmsg, id) + idlen, msg->dlen);
    return msg;
}

/*
 * @param idlen: The length of the rlfsmsg's ID field
 * @param dlen: The length of the rlfsmsg's data field
 *
 * Notes:
 *   Remember to use freemsg() to free after use.
 */
rlfsmsg *mallocmsg(unsigned idlen, unsigned dlen) {
    rlfsmsg *msg = (rlfsmsg *)malloc(sizeof(rlfsmsg));
    if(msg == NULL) {
        logmsg(stderr, FLOC"rlfsmsg allocates failed!\n", LOC);
        return NULL;
    }
    memset(msg->magic, 0, sizeof(msg->magic));
    strcpy(msg->magic, "RLFS");
    msg->id = (unsigned char *)malloc(idlen);
    memset(msg->id, 0, idlen);
    msg->idlen = idlen;
    msg->type = MSG_NULL;
    msg->data = (unsigned char *)malloc(dlen);
    memset(msg->data, 0, dlen);
    msg->dlen = dlen;
    msg->action = ACTION_NULL;
    return msg;
}

void freemsg(rlfsmsg *msg) {
    free(msg->id);
    free(msg->data);
    free(msg);
}

void printmsg(rlfsmsg *msg) {
    logmsg(stdout, FLOC"----- MESSAGE DISSECT -----\n", LOC);
    printf("msg->magic:%s\n", msg->magic);
    printf("msg->action: %#02x\n", msg->action);
    printf("msg->idlen: %u\n", msg->idlen);
    printf("msg->dlen: %u\n", msg->dlen);
    switch(msg->type) {
        case MSG_FILEMETA:
            printf("msg->type: MSG_FILEMETA\n");
            printf("msg->id: %s\n", msg->id);
            printf("msg->data: %u\n", *(unsigned *)(msg->data));
            break;
        case MSG_CHUNKDATA:
            printf("msg->type: MSG_CHUNKDATA\n");
            break;
        default:
            printf("Unknown message type!\n");
    }
}

/*
 * Send a buffer "buf" with a size "bufsz" to the socket "socketfd"
 */
int sendrlfs(int socketfd, unsigned char* buf, long bufsz) {
    uint32_t net_bufsz = htonl(bufsz);
    send(socketfd, &net_bufsz, sizeof(net_bufsz), 0);
    int ret = transfer_data(socketfd, TRANS_SEND, buf, bufsz);
    if(ret == -1) {
        logmsg(stderr, FLOC"Send RLFS error!\n", LOC);
        return -1;
    }
    return 0;
}

/*
 * Receive a buffer from socket
 *
 * Notes:
 *   Remember to free after use.
 */
unsigned char *recvrlfs(int socketfd) {
    uint32_t net_bufsz;
    recv(socketfd, &net_bufsz, sizeof(net_bufsz), 0);
    long bufsz = ntohl(net_bufsz);
    unsigned char *buf = (unsigned char *)malloc(bufsz);
    memset(buf, 0, bufsz);
    int ret = transfer_data(socketfd, TRANS_RECV, buf, bufsz);
    if(ret == -1) {
        logmsg(stderr, FLOC"Recv RLFS error!\n", LOC);
        free(buf);
        return NULL;
    }
    logmsg(0, FLOC"Recv data is done!\n", LOC);
    return buf;
}

/*
 * Send an integer to the socket
 */
int sendint(int socketfd, int val) {
    int net_val = htonl(val);
    send(socketfd, &net_val, sizeof(net_val), 0);
    return 0;
}

int recvint(int socketfd) {
    int net_val;
    recv(socketfd, &net_val, sizeof(int), 0);
    int val = ntohl(net_val);
    return val;
}

/*
 * Check response status from socket
 *
 * Return:
 *  return 0 if good otherwise -1.
 */
int checkstatus(int socketfd) {
    int ret = recvint(socketfd);
    switch(ret) {
        case ERR_RMDIR:
            logmsg(stderr, FLOC"Server cannot remove existed directory!\n", LOC);
            ret = -1;
            break;
        case ERR_NEWDIR:
            logmsg(stderr, FLOC"Server cannot remove existed directory!\n", LOC);
            ret = -1;
            break;
        case ERR_OK:
            ret = 0;
            break;
        default:
            logmsg(stderr, FLOC"What the hell is the message!\n", LOC);
            ret = -1;
    }
    return ret;
}

int sendfile(int socketfd, const char *fpath) {
    FILE *f = fopen(fpath, "rb");
    if(f == NULL) {
        logmsg(stdout, FLOC"%s\n", LOC, "Open file failed!");
        fclose(f);
        return -1;
    }

    // Send file size
    fseek(f, 0, SEEK_END);
    long fsz = ftell(f);
    if(fsz <= 0) {
        logmsg(stdout, FLOC"%s\n", LOC, "Zero file will not be sent!");
        return -1;
    }
    uint32_t net_fsz = htonl(fsz);
    send(socketfd, &net_fsz, sizeof(net_fsz), 0);

    // Send file
    rewind(f);
    unsigned char packet[MAX_PACKET_LEN];
    long remain_fsz = fsz;
    while(remain_fsz > 0) {
        size_t packetsz = remain_fsz > MAX_PACKET_LEN ? MAX_PACKET_LEN : remain_fsz;
        fread(packet, 1, packetsz, f);

        // Send a packet
        size_t remain_packetsz = packetsz;
        unsigned char *ppacket = packet;
        while(remain_packetsz > 0) {
            int sendsz = send(socketfd, ppacket, remain_packetsz, 0);
            if(sendsz == -1) {
                logmsg(stdout, FLOC"%s error: %s\n", LOC, "Send file failed!",
                        strerror(errno));
                return -1;
            }
            remain_packetsz -= sendsz;
            ppacket += sendsz;
        }
        remain_fsz -= packetsz;
    }
    fclose(f);
    return 0;
}

int recvfile(int socketfd, const char *fpath) {
    // Open file
    FILE *f = fopen(fpath, "wb");
    if(f == NULL) {
        logmsg(stdout, FLOC"%s\n", LOC, "Open file failed!");
        return -1;
    }

    // Get file size
    uint32_t net_fsz;
    recv(socketfd, &net_fsz, sizeof(net_fsz), 0);
    long fsz = ntohl(net_fsz);

    // Receive file
    unsigned char packet[MAX_PACKET_LEN];
    long remain_fsz = fsz;
    while(remain_fsz > 0) {
        int to_recv_bytes = remain_fsz > sizeof(packet) ? sizeof(packet) : remain_fsz;
        int recv_bytes = recv(socketfd, packet, to_recv_bytes, 0);
        fwrite(packet, 1, recv_bytes, f);
        while(recv_bytes < to_recv_bytes) {
            int cur_recv_bytes = recv(socketfd, packet, to_recv_bytes - recv_bytes, 0);
            recv_bytes += cur_recv_bytes;
            fwrite(packet, 1, cur_recv_bytes, f);
        }
        remain_fsz -= to_recv_bytes;
    }
    fclose(f);
    return 0;
}

/*
 * Transfer data between the server and client
 *
 * @param socketfd: The active socket
 * @param type: To receive, set it to TRANS_RECV; To send, set it to TRANS_SEND.
 * @param buf: If type is TRANS_RECV, then send the data in the buffer to the other side,
 *     or else receive data and store into the buffer.
 * @param len: The buffer size.
 *
 * Return:
 *   0 if ok, -1 if wrong.
 */


int transfer_data(int socketfd, int type, void *buf, int len) {
    int trans_bytes;
    if(type == TRANS_RECV) {
        trans_bytes = recv(socketfd, buf, len, 0);
    } else if(type == TRANS_SEND) {
        trans_bytes = send(socketfd, buf, len, 0);
    } else {
        return -1;
    }
    if(trans_bytes == -1) return -1;
    while(trans_bytes < len) {
        int cur_trans_bytes;
        if(type == TRANS_RECV) {
            cur_trans_bytes = recv(socketfd, buf + trans_bytes, len - trans_bytes, 0);
        } else if(type == TRANS_SEND) {
            cur_trans_bytes = send(socketfd, buf + trans_bytes, len - trans_bytes, 0);
        } else {
            return -1;
        }
        if(cur_trans_bytes == -1) return -1;
        trans_bytes += cur_trans_bytes;
    }
    return 0;
}

/*
 * Connect to a RLFS node
 *
 * Notes:
 *   From https://github.com/libfuse/sshfs with lightly modification
 *
 */
static int connect_node(rlfsnode* node) {
    char* host = node->host;
    char* port = node->port;

    node->live = 0;

	int err;
	int sock;
	int opt;
	struct addrinfo *ai;
	struct addrinfo hint;

	memset(&hint, 0, sizeof(hint));
	hint.ai_family = PF_INET;
	hint.ai_socktype = SOCK_STREAM;
	err = getaddrinfo(host, port, &hint, &ai);
	if (err) {
		fprintf(stderr, "failed to resolve %s:%s: %s\n", host, port,
			gai_strerror(err));
		return -1;
	}
	sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
	if (sock == -1) {
		perror("failed to create socket");
		return -1;
	}
	err = connect(sock, ai->ai_addr, ai->ai_addrlen);
	if (err == -1) {
		perror("failed to connect");
		return -1;
	}
	opt = 1;
	err = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
	if (err == -1)
		perror("warning: failed to set TCP_NODELAY");

	freeaddrinfo(ai);

    node->live = 1;
    return sock;
}


/*
 * Load node from node configuration file
 *
 * @param nodefile The path to the configuration file
 *
 * Return:
 *   If no valid node, then return -1. Otherwise 0.
 *
 * Notes:
 *  One line for a node. The line should firstly contains the host and then the
 *  port with one or more spaces separated. Any line starting with '#' will be
 *  treated as comment line. An example of configuration is showed as following:
 *
 *  # -------------------------------
 *  # The first host
 *   192.168.56.7 3490
 *  # The second host
 *   192.168.56.9 3490
 *  # Well, a blank line
 *
 *  # The last host
 *   192.168.56.6 3490
 *  # -------------------------------
 *
 */
int read_node_conf(char* nodefile) {
    FILE *fp = NULL;
    fp = fopen(nodefile, "r");
    if(NULL == fp) {
        logmsg(stderr, FLOC"Open config file failed!", LOC);
        fclose(fp);
        return -1;
    }
    char line[MAX_LINE_SIZE + 1] = {0};
    int nodeno = 0, livenode = 0;
    bool has_metanode = false;
    while(fgets(line, sizeof(line) / sizeof(char), fp) && nodeno < MAX_NODE_NUM) {
        char line_stripped[MAX_LINE_SIZE] = {0};
        str_strip(line, line_stripped);
        if(strlen(line_stripped) == 0 || line_stripped[0] == '#') continue;
        sscanf(line_stripped, "%s %s", config.nodejar[nodeno].host,
                config.nodejar[nodeno].port);
        int wfd = connect_node(&config.nodejar[nodeno]);
        if(wfd > 0) {
            if(!has_metanode) {
                config.metanodeid = nodeno;
                has_metanode = true;
            }
            ++livenode;
            config.nodejar[nodeno].live = 1;
            char buf[] = "TEST";
            sendrlfs(wfd, buf, sizeof(buf));
            close(wfd);
        } else {
            config.nodejar[nodeno].live = 0;
        }
        ++nodeno;
    }
    config.nodenum = nodeno;
    fclose(fp);
    if(config.nodenum == 0 || livenode == 0) return -1;
    else return 0;
}

/*
 * Initialize the global config structure
 */
void initconfig() {
    memset(config.root, 0, PATH_MAX);
    memset(config.nodejar, 0, sizeof(config.nodejar));
    config.nodenum = 0;
    config.thread = MAX_THREAD_NUM / 2;
    config.metanodeid = -1;
    memset(config.nodeconf, 0, PATH_MAX);

    // Create local temporary directory
    const char *rlfstmp = getenv("RLFSROOT");
    if(rlfstmp == NULL) rlfstmp = getenv("HOME");
    else createdir(rlfstmp, CREATE_IFNEXIST);
    strncpy(config.rlfstmp, rlfstmp, PATH_MAX);
    config.rlfstmp[PATH_MAX - 1] = '\0';
    strncat(config.rlfstmp, "/.rlfs", PATH_MAX - strlen(config.rlfstmp));
    createdir(config.rlfstmp, CREATE_IFNEXIST);
}

/*
 * Read file and split it into chunks
 *
 * @param fp: A file descriptor to the file.
 * @param fc: A filechunk structure to hold a chunk.
 *
 * Note:
 *   If the file descriptor reaches the end of the file,
 *   then it return 0, or else 1. You may regard the function
 *   as an iterator.
 */

int nextchunk(FILE *fp, filechunk* fc) {
    size_t bytes_read = fread(fc->data, 1, FILE_CHUNK, fp);
    fc->datalen = bytes_read;
    if(!feof(fp)) return 1;
    else return 0;
}

/*
 * Notes:
 *   Remember to use free_file_chunk to free after use
 */
filechunk *malloc_filechunk() {
    filechunk *fc = (filechunk *)malloc(sizeof(filechunk));
    fc->data = (unsigned char *)malloc(FILE_CHUNK);
    if(fc->data == NULL) {
        logmsg(stderr, FLOC"New file chunk failed!\n", LOC);
        free(fc);
        return NULL;
    } else {
        fc->datalen = 0;
        return fc;
    }
}

void free_file_chunk(filechunk *fc) {
    free(fc->data);
    free(fc);
}

/*
 * Remove files(used by nftw())
 */
static int rmfiles(const char *pathname, const struct stat *sbuf,
        int type, struct FTW *ftwb) {
    if(remove(pathname) < 0) {
        logmsg(stderr, "Cannot remove files!\n");
        return -1;
    }
    return 0;
}

/*
 * Get chunk from storage nodes
 */
void *getchunk(void *arg) {
    chunkmeta *cm = (chunkmeta *)arg;
    logmsg(0, FLOC"cm->sz: %u "
                  "cm->id: %u "
                  "cm->host: %s "
                  "cm->port: %s\n", LOC, cm->sz, cm->id, cm->host, cm->port);
    const char *filepath = (const char *)arg + sizeof(chunkmeta);
    logmsg(0, FLOC"filepath: %s\n", LOC, filepath);

    rlfsnode node;
    memcpy(node.host, cm->host, sizeof(node.host));
    memcpy(node.port, cm->port, sizeof(node.port));

    char dup_filepath[strlen(filepath) + 1];
    memcpy(dup_filepath, filepath, strlen(filepath) + 1);
    const char *bare_filename = basename(dup_filepath);
    char filename[strlen(bare_filename) + 2];
    filename[0] = '/';
    memcpy(filename + 1, bare_filename, strlen(bare_filename) + 1);

    logmsg(0, FLOC"Base name: %s\n", LOC, filename);
    unsigned idlen = strlen(filename) + 1;
    rlfsmsg *msg = (rlfsmsg *)mallocmsg(idlen, sizeof(cm->id));
    msg->type = MSG_CHUNKDATA;
    msg->action = ACTION_CHUNK_FETCH;
    memcpy(msg->id, filename, msg->idlen);
    memcpy(msg->data, &cm->id, msg->dlen);

    unsigned msgsz = get_serialize_size(msg);
    unsigned char *msgbin = serialize_msg(msg); freemsg(msg);
    int wfd = connect_node(&node);
    sendrlfs(wfd, msgbin, msgsz); free(msgbin);

    char *perffile = (char *)arg + sizeof(chunkmeta) + strlen(filepath) + 1;
    logmsg(0, FLOC"Perf file full path: %s\n", LOC, perffile);

    // Recv chunk data
    write_perf_time_beg(perffile, cm->id, gettimems());
    unsigned char *chunkdata = recvrlfs(wfd);
    write_perf_time_end(perffile, cm->id, gettimems());
    FILE *fp = fopen(filepath, "rb+");
    fseek(fp, cm->id * FILE_CHUNK, SEEK_SET);
    fwrite(chunkdata, 1, cm->sz, fp); free(chunkdata);
    fclose(fp);

    free(arg);
    return NULL;
}

/*
 * Send chunk to storage node
 */
void *sendchunk(void *arg) {
    char *parg = arg;
    rlfsnode node;
    memcpy(node.host, parg, sizeof(node.host)); parg += sizeof(node.host);
    memcpy(node.port, parg, sizeof(node.port)); parg += sizeof(node.port);
    char path[PATH_MAX] = {0}; memcpy(path, parg, strlen(parg) + 1);
    parg += strlen(parg) + 1;
    unsigned cid = *(unsigned *)parg; parg += sizeof(cid);
    unsigned chunksz = *(unsigned *)parg; parg += sizeof(chunksz);
    unsigned char *chunkdata = (unsigned char *)malloc(chunksz);
    memcpy(chunkdata, parg, chunksz);
    free(arg);

    logmsg(0, FLOC"Thread[%d] starts at %llu...\n", LOC, cid, gettimems());

    rlfsmsg *chunkmsg = (rlfsmsg *)mallocmsg(strlen(path) + 1, sizeof(cid) + chunksz);
    memcpy(chunkmsg->id, path, chunkmsg->idlen);
    memcpy(chunkmsg->data, &cid, sizeof(cid));
    memcpy(chunkmsg->data + sizeof(cid), chunkdata, chunksz); free(chunkdata);
    chunkmsg->type = MSG_CHUNKDATA;
    chunkmsg->action = ACTION_CHUNK_STORE;
    unsigned chunkmsgsz = get_serialize_size(chunkmsg);
    unsigned char *chunkmsg_bin = serialize_msg(chunkmsg); freemsg(chunkmsg);

    // if(cid % 2 == 0) sleep(2);

    char perffile[PATH_MAX] = {0};
    get_perfname(path + 1, "send", perffile);

    int wfd = connect_node(&node);
    write_perf_time_beg(perffile, cid, gettimems());
    sendrlfs(wfd, chunkmsg_bin, chunkmsgsz);
    write_perf_time_end(perffile, cid, gettimems());
    free(chunkmsg_bin);
    close(wfd);

    logmsg(0, FLOC"Thread[%u] ends!\n", LOC, cid);

    // Send chunk meta info to meta node
    logmsg(0, FLOC"Send meta info ...\n", LOC);
    rlfsmsg *metamsg = (rlfsmsg *)mallocmsg(strlen(path) + 1, sizeof(chunkmeta));
    memcpy(metamsg->id, path, metamsg->idlen);
    metamsg->type = MSG_FILEMETA;
    metamsg->action = ACTION_META_FILL;
    chunkmeta cm; memset(&cm, 0, sizeof(cm));
    cm.id = cid;
    cm.sz = chunksz;
    memcpy(cm.host, node.host, sizeof(cm.host));
    memcpy(cm.port, node.port, sizeof(cm.port));
    memcpy(metamsg->data, &cm, metamsg->dlen);
    rlfsnode metanode = config.nodejar[config.metanodeid];
    int metawfd = connect_node(&metanode);
    unsigned metamsgsz = get_serialize_size(metamsg);
    unsigned char *metamsgbin = serialize_msg(metamsg); freemsg(metamsg);
    sendrlfs(metawfd, metamsgbin, metamsgsz); free(metamsgbin);
    close(metawfd);
    return NULL;
}


int fill_chunkmeta_mtsafe(const char *fpath, chunkmeta *cm, unsigned off) {
    // Notice that for writing binary file from some offset with some bytes, you
    // should set the mode to "rb+" but ot "ab+"
    FILE *fp = fopen(fpath, "rb+");
    fseek(fp, 0, SEEK_END);
    if(ftell(fp) < off + (cm->id + 1) * sizeof(chunkmeta)) {
        logmsg(stderr, FLOC"The length of file %s is too short!\n", LOC, fpath);
        fclose(fp);
        return -1;
    }
    fseek(fp, off + cm->id * sizeof(chunkmeta), SEEK_SET);
    fwrite(cm, 1, sizeof(chunkmeta), fp);
    fclose(fp);
    return 0;
}

int zerofile(const char *path, long sz) {
    FILE *fp = fopen(path, "wb");
    if(fp == NULL) {
        logmsg(stderr, FLOC"Cannot open file [%s] to fill zero!\n", LOC, path);
        return -1;
    }
    const long blksz = 1 << 20;
    char block[blksz];
    memset(block, 0, blksz);
    long remainsz = sz;
    while(remainsz > blksz) {
        fwrite(block, 1, blksz, fp);
        remainsz -= blksz;
    }
    fwrite(block, 1, remainsz, fp);
    fclose(fp);
    return 0;
}

/*
 * Create a directory
 *
 * @param dpath: The directory path.
 * @param way:
 *     CREATE_FORCE_IFEXIST: If the directory exists, remove it and then create one
 *     CREATE_IFNEXIST: If the directory does not exists, create one
 * Return: 0 for ok, -1 for errors.
 */

int createdir(const char *dpath, int way) {
    DIR *dir = NULL;
    switch(way) {
        case CREATE_FORCE_IFEXIST:
            ;
            dir = opendir(dpath);
            if(dir != NULL) {
                if(nftw(dpath, rmfiles, 10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS) < 0) {
                    logmsg(stderr, FLOC"Cannot remove directory: %s\n", dpath);
                    closedir(dir);
                    return -1;
                }
            }
            closedir(dir);
        case CREATE_IFNEXIST:
            ;
            dir = opendir(dpath);
            if(dir == NULL) {
                if(mkdir(dpath, 0770) == 0) {
                    logmsg(0, FLOC"Create %s successfully!\n", LOC, dpath);
                } else {
                    logmsg(stderr, FLOC"Create %s failed!\n", LOC, dpath);
                    closedir(dir);
                    return -1;
                }
            }
            closedir(dir);
            break;
    }
    return 0;
}

int createperf(const char *perfpath, unsigned recordnum, unsigned recordsz) {
    zerofile(perfpath, recordsz * recordnum);
    return 0;
}

/*
 * Write the beginning time(in ms) into the perf file
 */
int write_perf_time_beg(char *perffile, unsigned recordid, long long starttm) {
    FILE *fp = fopen(perffile, "rb+");
    fseek(fp, recordid * (sizeof(starttm) * 2), SEEK_SET);
    fwrite(&starttm, 1, sizeof(starttm), fp);
    fclose(fp);
    return 0;
}

int write_perf_time_end(char *perffile, unsigned recordid, long long endtm) {
    FILE *fp = fopen(perffile, "rb+");
    fseek(fp, recordid * (sizeof(endtm) * 2) + sizeof(endtm), SEEK_SET);
    fwrite(&endtm, 1, sizeof(endtm), fp);
    fclose(fp);
    return 0;
}

void get_perfname(char *file_fullname, char *direction, char perffile[PATH_MAX]) {
    snprintf(perffile, PATH_MAX, "%s/%s.%s.%d.perf",
            config.rlfstmp, file_fullname, direction, FILE_CHUNK);
}

long long gettimems() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    long long ms = tv.tv_sec * 1000LL + tv.tv_usec / 1000;
    return ms;
}

#endif  // #define RLFS_H
