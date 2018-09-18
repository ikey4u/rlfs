#define _XOPEN_SOURCE 600 //FTW_DEPTH, FTW_MOUNT ...

#include <stdio.h>
#include <stdlib.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "rlfs.h"
#include "util.hpp"

#define PORT "3490"  // the port users will be connecting to
#define BACKLOG 5	 // how many pending connections queue will hold

// The user should could set an environment 'RLFSROOT' to indicate where to save data in
// the server, if the environment is not set, the default root path will be
// $HOME
const char *RLFS_ROOT_ENV = "RLFSROOT";
const char *RLFS_DISK_PATH = "/rlfsdisk"; // Storage node's disk directory
const char *RLFS_META_PATH = "/rlfsmeta"; // Meta node's meta directory

void sigchld_handler(int s) {
	(void)s; // quiet unused variable warning
	int saved_errno = errno; // waitpid() might overwrite errno, so we save and restore it
	while(waitpid(-1, NULL, WNOHANG) > 0);
	errno = saved_errno;
}

/*
 * Get sockaddr, IPv4 or IPv6:
 */
void *get_in_addr(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int main(void) {
	int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
	struct addrinfo hints, *servinfo, *p;
	struct sockaddr_storage their_addr; // connector's address information
	socklen_t sin_size;
	struct sigaction sa;
	int yes=1;
	char s[INET6_ADDRSTRLEN];
	int rv;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	if ((rv = getaddrinfo(NULL, PORT, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and bind to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			perror("server: socket");
			continue;
		}
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
				sizeof(int)) == -1) {
			perror("setsockopt");
			exit(1);
		}
		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			perror("server: bind");
			continue;
		}
		break;
	}

	freeaddrinfo(servinfo); // all done with this structure

	if (p == NULL)  {
		fprintf(stderr, "server: failed to bind\n");
		exit(1);
	}

	if (listen(sockfd, BACKLOG) == -1) {
		perror("listen");
		exit(1);
	}

	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		perror("sigaction");
		exit(1);
	}

	while(1) {
		sin_size = sizeof their_addr;
        logmsg(stdout, FLOC"Wating for new connection ...\n", LOC);
		new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
		if (new_fd == -1) {
			perror("accept");
			continue;
		}
        logmsg(0, "server: got connection from %s new_fd = %d sockfd = %d\n", LOC, s, new_fd, sockfd);
        pid_t frk = fork();
        if(frk < 0) {
            logmsg(stderr, FLOC"Fork error!\n", LOC);
        } else if(frk == 0) {
            close(sockfd); // child doesn't need the listener

            unsigned char *msgbin = recvrlfs(new_fd);
            if(msgbin == NULL) {
                logmsg(stderr, FLOC"No data is received\n", LOC);
                close(new_fd);
                exit(1);
            }

            // Test how many nodes are live
            if(strncmp(msgbin, "TEST", 5) == 0) {
                close(new_fd);
                logmsg(0, FLOC"Test Connection! Closed!\n", LOC);
                exit(0);
            }

            rlfsmsg *msg = deserialize_msg(msgbin);
            free(msgbin);
            if(strcmp(msg->magic, "RLFS") != 0) {
                logmsg(stderr, FLOC"RLFS does not recognize the message!\n", LOC);
                close(new_fd);
                exit(1);
            }
            int ret = 0;
            int chunk_num = 0;

            char rlfsroot[PATH_MAX] = {0};
            const char *prlfsroot = getenv(RLFS_ROOT_ENV);
            if(prlfsroot == NULL) prlfsroot = getenv("HOME");
            strncpy(rlfsroot, prlfsroot, PATH_MAX);
            rlfsroot[PATH_MAX - 1] = '\0';
            logmsg(0, FLOC"RLFS root path: %s\n", LOC, rlfsroot);

            switch(msg->type) {
                case MSG_FILEMETA:
                    ;
                    logmsg(0, FLOC"MSG_FILEMETA\n", LOC);
                    char metapath[PATH_MAX] = {0};
                    // Get META root path
                    strncpy(metapath, rlfsroot, PATH_MAX);
                    metapath[PATH_MAX - 1] = '\0';
                    strncat(metapath, RLFS_META_PATH, PATH_MAX - strlen(metapath));
                    if(createdir(metapath, CREATE_IFNEXIST) < 0) {
                        sendint(new_fd, ERR_NEWDIR);
                        close(new_fd);
                        exit(1);
                    }

                    if(msg->idlen != 0) {
                        // Get META file path
                        strncat(metapath, msg->id, PATH_MAX - strlen(metapath));
                        strncat(metapath, ".meta", PATH_MAX - strlen(metapath));
                        logmsg(0, FLOC"META File path: %s\n", LOC, metapath);
                    } else {
                        // The META ROOT path
                        logmsg(0, FLOC"META ROOT path: %s\n", LOC, metapath);
                    }

                    if(msg->action == ACTION_META_CREATE) {
                        logmsg(0, FLOC"ACTION_META_CREATE: [%s]\n", LOC, metapath);
                        // Get chunk num
                        long fsz = *(long *)msg->data;
                        unsigned chunk_num = fsz / FILE_CHUNK;
                        if(chunk_num * FILE_CHUNK < fsz) chunk_num += 1;
                        FILE *fmeta = fopen(metapath, "wb");
                        fwrite(&fsz, 1, sizeof(fsz), fmeta);
                        fwrite(&chunk_num, 1, sizeof(chunk_num), fmeta);

                        // Zeros the META record
                        unsigned metachunk_record_sz = sizeof(chunkmeta) * chunk_num;
                        unsigned char *metabuf = (unsigned char *)malloc(metachunk_record_sz);
                        memset(metabuf, 0, metachunk_record_sz);
                        fwrite(metabuf, 1, metachunk_record_sz, fmeta); free(metabuf);
                        fclose(fmeta);

                        // Send reply
                        sendint(new_fd, ERR_OK);
                    } else if(msg->action == ACTION_META_FILL) {
                        logmsg(0, FLOC"ACTION_META_FILL\n", LOC);
                        chunkmeta *cm = (chunkmeta *)msg->data;
                        logmsg(0, FLOC"cm->sz: %u "
                                      "cm->id: %u "
                                      "cm->host: %s "
                                      "cm->port: %s\n", LOC, cm->sz, cm->id, cm->host, cm->port);
                        // The first two fields of a filemeta are the size of the
                        // file(a long variable) and the chunk count(a unsigned
                        // integer)
                        unsigned filemeta_off = sizeof(long) + sizeof(unsigned);
                        fill_chunkmeta_mtsafe(metapath, cm, filemeta_off);
                    } else if(msg->action == ACTION_META_FETCH) {
                        logmsg(0, FLOC"ACTION_META_FETCH\n", LOC);
                        if(msg->idlen == 0) {
                            // Fetch all files
                            DIR *dir = opendir(metapath);
                            if(dir) {
                                struct dirent *ent = readdir(dir);
                                int client_ok = 0;
                                char suffix[] = ".meta";
                                int nsuffix = strlen(suffix);
                                while(ent) {
                                    int nname = strlen(ent->d_name);
                                    if(nname > nsuffix && strncmp(ent->d_name + nname - nsuffix, suffix, nsuffix) == 0) {
                                        sendint(new_fd, 1);
                                        sendrlfs(new_fd, ent->d_name, strlen(ent->d_name) + 1);
                                        int pmetaroot = strlen(metapath);
                                        strncat(metapath, "/", PATH_MAX - strlen(metapath));
                                        strncat(metapath, ent->d_name, PATH_MAX - strlen(metapath));
                                        logmsg(0, FLOC"Sending meta file: [%s]\n", LOC, metapath);
                                        sendfile(new_fd, metapath);
                                        metapath[pmetaroot] = '\0';
                                    }
                                    ent = readdir(dir);
                                }
                                logmsg(0, FLOC"File meta is sent!\n", LOC);
                                sendint(new_fd, 0);
                                closedir(dir);
                            } else {
                                logmsg(stderr, FLOC"Cannot open directory %s!", LOC, metapath);
                                close(new_fd);
                                exit(1);
                            }
                        } else {
                            // Fetch one file when the user accesses for better
                            // performance
                            // TODO(bugnoree 2018-09-08): This feature is not implemented
                            // for now
                            ;
                        }
                    }
                    break;
                case MSG_CHUNKDATA:
                    ;
                    logmsg(0, FLOC"Deal with chunk data ......\n", LOC);
                    // Get node disk root
                    char chunkpath[PATH_MAX] = {0};
                    strncpy(chunkpath, rlfsroot, PATH_MAX);
                    chunkpath[PATH_MAX - 1] = '\0';
                    strncat(chunkpath, RLFS_DISK_PATH, PATH_MAX - strlen(chunkpath));
                    logmsg(0, FLOC"RLFS node disk path:%s\n", LOC, chunkpath);
                    /*
                     * mkdir is MT-Safe, so we do not worry about the directory creation.
                     * See http://www.gnu.org/software/libc/manual/html_node/Creating-Directories.html
                     * for more details.
                     */
                    if(createdir(chunkpath, CREATE_IFNEXIST) < 0) {
                         close(new_fd);
                         exit(1);
                    }

                    // Get file's chunk directory
                    strncat(chunkpath, msg->id, PATH_MAX - strlen(chunkpath));
                    logmsg(0, FLOC"Chunk directory:%s\n", LOC, chunkpath);
                    if(createdir(chunkpath, CREATE_IFNEXIST) < 0) {
                         close(new_fd);
                         exit(1);
                    }

                    // Get chunk sequence number length, see http://ahageek.com/blog/algo/leetcode/007-reverse-integer/index.html
                    // for more details, and convert it into a string
                    unsigned chunkidlen = 0;
                    unsigned chunkid = *(unsigned *)msg->data;
                    if(chunkid == 0) chunkidlen = 1;
                    else chunkidlen = floor(log10(chunkid * 1.0)) + 1;
                    char *chunkname = (char *)malloc(chunkidlen + 1);
                    memset(chunkname, 0, chunkidlen + 1);
                    snprintf(chunkname, chunkidlen + 1, "%u", chunkid);

                    // Get chunk data full path
                    strncat(chunkpath, "/", PATH_MAX - strlen(chunkpath));
                    strncat(chunkpath, chunkname, PATH_MAX - strlen(chunkpath));
                    free(chunkname);

                    // Save chunk data
                    if(msg->action == ACTION_CHUNK_STORE) {
                        logmsg(0, FLOC"ACTION_CHUNK_STORE\n", LOC);
                        FILE *fchunk = fopen(chunkpath, "wb");
                        unsigned char *chunk = msg->data + sizeof(unsigned);
                        fwrite(chunk, 1, msg->dlen, fchunk);
                        fclose(fchunk);
                        logmsg(0, FLOC"Chunk [%s] has been saved!\n", LOC, chunkpath);
                    } else if(msg->action == ACTION_CHUNK_FETCH) {
                        logmsg(0, FLOC"ACTION_CHUNK_FETCH\n", LOC);
                        FILE *fchunk = fopen(chunkpath, "rb");
                        unsigned char *chunkdata = (unsigned char *)malloc(FILE_CHUNK);
                        fread(chunkdata, 1, FILE_CHUNK, fchunk);
                        struct stat stbuf;
                        stat(chunkpath, &stbuf);
                        sendrlfs(new_fd, chunkdata, stbuf.st_size);
                        free(chunkdata);
                        fclose(fchunk);
                        logmsg(0, FLOC"Chunk [%s] has been sent!\n", LOC, chunkpath);
                    }
                    break;
                default:
                    logmsg(stderr, FLOC"Unknown message type!\n", LOC);
            }
            logmsg(0, FLOC"MSG is processed!\n", LOC);
            freemsg(msg);
            close(new_fd);
            exit(0);
        } else {
            logmsg(0, FLOC"Subprocess ID: %d\n", LOC, frk);
            close(new_fd);  // parent doesn't need this
        }
    }
	return 0;
}
