#include "bbfsop.hpp"
#include "rlfs.h"
#include "util.hpp"

extern rlfsconfig config;

/*
 * Release an open file
 */
int rlfs_release(const char *path, struct fuse_file_info *fi) {
    logmsg(0, FLOC"Close file %s fd = %d\n", LOC, path, fi->fh);
    log_msg("\nbb_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    log_fi(fi);

    char fpath[PATH_MAX];
    bb_fullpath(fpath, path);
    if(fi->flags & (O_WRONLY | O_CREAT)) {
        /*
         * Send file meta
         */
        rlfsmsg *msg = mallocmsg(strlen(path) + 1, sizeof(long));
        msg->type = MSG_FILEMETA;
        msg->action = ACTION_META_CREATE;
        memcpy(msg->id, path, msg->idlen);
        struct stat stbuf;
        fstat(fi->fh, &stbuf);
        long fsz = stbuf.st_size;
        memcpy(msg->data, &fsz, msg->dlen);
        unsigned char *msgbin = serialize_msg(msg);
        unsigned msgsz = get_serialize_size(msg); freemsg(msg);
        // Connect to meta node and send file meta
        rlfsnode *metanode = &config.nodejar[config.metanodeid];
        int wfd = connect_node(metanode);
        logmsg(0, FLOC"Send RLFS message[file meta] ...\n", LOC);
        sendrlfs(wfd, msgbin, msgsz); free(msgbin);
        logmsg(0, FLOC"Send RLFS message[file meta] DONE!\n", LOC);

        // Check file meta reply
        int ret = checkstatus(wfd);
        if(ret != 0) {
            return -1;
            close(wfd);
        }

        /*
         * Send file
         */
        logmsg(0, FLOC"Send RLFS message[file] ...\n", LOC);

        unsigned chunknum = fsz / FILE_CHUNK;
        if(chunknum * FILE_CHUNK < fsz) chunknum += 1;
        int has_next_chunk = 1;
        int nodeidx = -1;
        logmsg(0, FLOC"chunknum = %u\n", LOC, chunknum);

        char perffile[PATH_MAX] = {0};
        char file_fullname[NAME_MAX] = {0};
        memcpy(file_fullname, path + 1, NAME_MAX);
        get_perfname(file_fullname, "send", perffile);
        logmsg(0, FLOC"Perf file (send) full path: %s\n", LOC, perffile);
        createperf(perffile, chunknum, 2 * sizeof(long long));

        unsigned chunkid = 0;
        pthread_t threads[chunknum];
        memset(threads, 0, sizeof(pthread_t) * chunknum);

        FILE *fp = fopen(fpath, "rb");
        /*
         * There is a subtle bug here(of course, it has been solved!).
         * When the file size is not a multiple of FILE_CHUNK, it works well, but when it is
         * not, the bug shows its savage face. Why? The function nextchunk could be regarded
         * as an iterator, the last second file reading reaches the end, but nextchunk reports
         * has_next_chunk is true(since it really does not reach the end of the file) in
         * this situation. So the pthread_create will work hard to create a new thread to
         * send the chunk(it is weird that the program does not crash at pthread_create
         * since the array is out of bound). As a result, the pthread_join may join the
         * thread which has been joined! This causes a segment fault error at pthread_join!
         * it costs me several hours to figure it out ...
         *
         * The solution is really simple, just add "chunkid < chunknum" in the loop.
         */
        while(has_next_chunk && chunkid < chunknum) {
            logmsg(0, FLOC"Send chunk %u ... \n", LOC, chunkid);

            filechunk *fc = malloc_filechunk();
            has_next_chunk = nextchunk(fp, fc);

            nodeidx = next_live_nodeno(nodeidx);
            if(nodeidx == -1) break;

            rlfsnode node = config.nodejar[nodeidx];
            unsigned argbufsz = sizeof(node.host) + sizeof(node.port) + (strlen(path) + 1)
                + sizeof(chunkid) + sizeof(fc->datalen) + fc->datalen;
            // Free in the subthread
            unsigned char *argbuf = (unsigned char *)malloc(argbufsz);
            char *parg = argbuf;
            memcpy(parg, node.host, sizeof(node.host)); parg += sizeof(node.host);
            memcpy(parg, node.port, sizeof(node.port)); parg += sizeof(node.port);
            memcpy(parg, path, strlen(path) + 1); parg += strlen(path) + 1;
            memcpy(parg, &chunkid, sizeof(chunkid)); parg += sizeof(chunkid);
            memcpy(parg, &fc->datalen, sizeof(fc->datalen)); parg += sizeof(fc->datalen);
            memcpy(parg, fc->data, fc->datalen); free_file_chunk(fc);

            // MT send chunk
            logmsg(0, "create pthread tid[%d]\n", chunkid);
            if(pthread_create(&threads[chunkid], NULL, sendchunk, argbuf) != 0) {
                threads[chunkid] = 0;
                logmsg(stderr,
                        FLOC"pthread_create() failed with tid = %d. errno = %d, %s\n",
                        LOC, chunkid, errno, strerror(errno));
                has_next_chunk = 0;
                break;
            }
            if((chunkid + 1) % config.thread == 0) {
                for(int j = chunkid + 1 - config.thread; j < chunkid + 1; ++j) {
                    void *retval;
                    pthread_join(threads[j], &retval);
                }
            }
            logmsg(0, "tid[%d] is created!\n", chunkid);
            ++chunkid;
        }
        logmsg(0, FLOC"Begin to join the threads, chunknum = %d ...\n", LOC, chunknum);
        for(int i = chunknum % config.thread; i > 0; --i) {
            void *retval;
            logmsg(0, FLOC"Joining %d ... \n", LOC, chunknum - i);
            if(pthread_join(threads[chunknum - i], &retval) != 0) {
                logmsg(stderr,
                        FLOC"pthread_join() failed with thread[%d], errno = %d, %s\n",
                        LOC, chunknum - i, errno, strerror(errno));
            }
        }
        logmsg(stdout, FLOC"File %s has been pushed to storage nodes!\n", LOC, fpath);
        fclose(fp);
    }
    // We need to close the file.  Had we allocated any resources
    // (buffers etc) we'd need to free them here as well.
    return log_syscall("close", close(fi->fh), 0);
}

/*
 * Initialize filesystem
 */
void *rlfs_init(struct fuse_conn_info *conn) {
    logmsg(0, FLOC, LOC);
    log_msg("\nbb_init()\n");

    log_conn(conn);
    log_fuse_context(fuse_get_context());

    // Retrieve segment files from remote nodes and merge them
    logmsg(0, FLOC"RLFS local storage: %s\n", LOC, config.rlfstmp);
    rlfsmsg *msg = (rlfsmsg *)mallocmsg(0, 0);
    msg->type = MSG_FILEMETA;
    msg->action = ACTION_META_FETCH;
    unsigned msgsz = get_serialize_size(msg);
    unsigned char *msgbin = serialize_msg(msg); freemsg(msg);
    rlfsnode *metanode = &config.nodejar[config.metanodeid];
    int metawfd = connect_node(metanode);
    sendrlfs(metawfd, msgbin, msgsz); free(msgbin);
    // unsigned char *buf = recvrlfs(metawfd);
    int hasfile = recvint(metawfd);
    while(hasfile) {
        unsigned char *pmetaname = recvrlfs(metawfd);
        unsigned char metaname[NAME_MAX] = {0};
        memcpy(metaname, pmetaname, NAME_MAX); free(pmetaname);

        logmsg(0, FLOC"metaname is [%s].\n", LOC, metaname);
        char metapath[PATH_MAX] = {0};
        strncat(metapath, config.rlfstmp, PATH_MAX - strlen(metapath));
        // Hide the meta file
        strncat(metapath, "/.", PATH_MAX - strlen(metapath));
        strncat(metapath, metaname, PATH_MAX - strlen(metapath));
        recvfile(metawfd, metapath);
        logmsg(0, FLOC"Save meta file to [%s]\n", LOC, metapath);

        FILE *fmeta = fopen(metapath, "rb");
        long fsz; fread(&fsz, 1, sizeof(fsz), fmeta);
        unsigned chunknum; fread(&chunknum, 1, sizeof(chunknum), fmeta);

        // Zero file storage
        char filepath[PATH_MAX] = {0};
        strncat(filepath, config.rlfstmp, PATH_MAX - strlen(filepath));
        strncat(filepath, "/", PATH_MAX - strlen(filepath));
        int filename_len = strlen(metaname) - strlen(".meta");
        int capofpath = PATH_MAX - strlen(filepath);
        int concatlen = filename_len > capofpath ? capofpath : filename_len;
        strncat(filepath, metaname,  concatlen);
        logmsg(0, FLOC"File path is [%s] \n", LOC, filepath);
        zerofile(filepath, fsz);

        // Create perf file
        char perffile[PATH_MAX] = {0};
        // metanem looks like tmux-2.7.tar.gz.meta
        logmsg(0, FLOC"metaname is [%s].\n", LOC, metaname);
        char file_fullname[NAME_MAX] = {0};
        get_barename(metaname, file_fullname);
        // logmsg(0, FLOC"file full name: [%s].\n", LOC, file_fullname);
        get_perfname(file_fullname, "recv", perffile);
        logmsg(0, FLOC"Perf file (recv) full path: %s\n", LOC, perffile);
        createperf(perffile, chunknum, 2 * sizeof(long long));

        pthread_t thread[chunknum];
        for(int i = 0; i < chunknum; ++i) {
            unsigned off = sizeof(fsz) + sizeof(chunknum) + i * sizeof(chunkmeta);
            fseek(fmeta, off, SEEK_SET);
            chunkmeta cm; fread(&cm, 1, sizeof(chunkmeta), fmeta);
            logmsg(0, FLOC"cm->sz: %u "
                          "cm->id: %u "
                          "cm->host: %s "
                          "cm->port: %s\n", LOC, cm.sz, cm.id, cm.host, cm.port);
            // Get file chunk
            // Will be freed in getchunk
            unsigned char *argbuf = (unsigned char *)malloc(sizeof(chunkmeta)
                    + (strlen(filepath) + 1) + (strlen(perffile) + 1));
            char *parg = argbuf;
            memcpy(parg, &cm, sizeof(chunkmeta)); parg += sizeof(chunkmeta);
            memcpy(parg, filepath, strlen(filepath) + 1); parg += strlen(filepath) + 1;
            memcpy(parg, perffile, strlen(perffile) + 1);
            pthread_create(&thread[i], NULL, getchunk, argbuf);
            if((i + 1) % config.thread == 0) {
                logmsg(0, FLOC"Wait %d - %d to stop ...\n", LOC, i + 1 - config.thread, i);
                for(int j = i + 1 - config.thread; j < i + 1; ++j) {
                    void *retval;
                    pthread_join(thread[j], &retval);
                }
            }
        }
        fclose(fmeta);
        remove(metapath);
        hasfile = recvint(metawfd);
        for(int i = chunknum % config.thread; i > 0; --i) {
            void *retval;
            logmsg(0, FLOC"[WHOLE] Wait %d to stop ...\n", LOC, chunknum - i);
            pthread_join(thread[chunknum - i], &retval);
        }
        logmsg(stdout, FLOC"File %s has been pulled to the local storage!\n", LOC, filepath);
    }
    close(metawfd);
    // Get all file meta counts
    return BB_DATA;
}



struct fuse_operations rlfs_operations = {
  .getattr = bb_getattr,
  .readlink = bb_readlink,
  // no .getdir -- that's deprecated
  .getdir = NULL,
  .mknod = bb_mknod,
  .mkdir = bb_mkdir,
  .unlink = bb_unlink,
  .rmdir = bb_rmdir,
  .symlink = bb_symlink,
  .rename = bb_rename,
  .link = bb_link,
  .chmod = bb_chmod,
  .chown = bb_chown,
  .truncate = bb_truncate,
  .utime = bb_utime,
  .open = bb_open,
  .read = bb_read,
  .write = bb_write,
  /** Just a placeholder, don't set */ // huh???
  .statfs = bb_statfs,
  .flush = bb_flush,
  .release = rlfs_release,
  .fsync = bb_fsync,

#ifdef HAVE_SYS_XATTR_H
  .setxattr = bb_setxattr,
  .getxattr = bb_getxattr,
  .listxattr = bb_listxattr,
  .removexattr = bb_removexattr,
#endif

  .opendir = bb_opendir,
  .readdir = bb_readdir,
  .releasedir = bb_releasedir,
  .fsyncdir = bb_fsyncdir,
  .init = rlfs_init,
  .destroy = bb_destroy,
  .access = bb_access,
  .ftruncate = bb_ftruncate,
  .fgetattr = bb_fgetattr
};
