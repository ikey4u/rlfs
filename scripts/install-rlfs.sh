#! /bin/bash
# Author: bugnofree
# Date: 2018-09-10
# Contact: pwnkeeper@gmail.com

set -e

cd $(realpath $(dirname "$0")) && cd ..
projdir=$(pwd)
cd ${projdir}
prefix=${HOME}/.local/rlfs
mkdir -p ${prefix}

FUSEROOT=$HOME/.local/fuse2
export PATH=$FUSEROOT/bin:$PATH
export CPATH=$FUSEROOT/include:$CPATH
export LD_LIBRARY_PATH=$FUSEROOT/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=$FUSEROOT/lib/pkgconfig:$PKG_CONFIG_PATH
export PATH=$HOME/.local/cmake/bin:$PATH

echo "[+] Installing the dependencies ..."
bash scripts/install-deps.sh || true
cp ${projdir}/scripts/fuse2env ${prefix}
echo "source ${prefix}/fuse2env" >> ${HOME}/.bashrc

echo "[+] Compiling the RLFS system ..."
mkdir -p ${projdir}/build && cd ${projdir}/build && cmake ..  && make

echo "[+] Installing the RLFS system ..."
cd ${projdir}/build && make install
cd ${projdir} && rm -rf ${projdir}/build
echo "export PATH=${prefix}/bin:\$PATH" >> ${HOME}/.bashrc

echo "[+] You are done! For client, run rlfsc, for server, run rlfsd!"
exec $SHELL
