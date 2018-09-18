#! /bin/bash

# Author: bugnofree
# Filename: install-deps.sh
# Date: 2018-09-18
# Contact: pwnkeeper@gmail.com

set -e

echo "[*] Get gettext autoconf libtool ..."
sudo apt install -y unzip gettext autoconf libtool pkg-config

prefixroot=$HOME/.local
pkgroot=$prefixroot/pkg
mkdir -p $pkgroot
mkdir -p $prefixroot

if [[ ! -f $prefixroot/OPENSSL ]]
then
    echo "[*] Get openssl ... "
    prefix=${prefixroot}/openssl
    sudo rm -rf $prefix || true
    mkdir -p $prefix
    zipname=OpenSSL_1_1_1-pre9
    mkdir -p ${prefix}/ssl
    if [[ ! -f ${pkgroot}/${zipname}.zip ]]
    then
        cd $pkgroot && wget https://codeload.github.com/openssl/openssl/zip/$zipname -O ${zipname}.zip
    fi
    cd $pkgroot && unzip ${zipname}.zip && cd openssl-${zipname}
    ./config --prefix=${prefix} --openssldir=${prefix}/ssl
    make && make install
    cd && rm -rf ${pkgroot}/openssl-${zipname}
    touch ${prefixroot}/OPENSSL
    echo "export PATH=${prefix}/bin:\$PATH" >> ${HOME}/.bashrc
    echo "openssl is done!"
fi

if [[ ! -f ${prefixroot}/FUSE2 ]]
then
    echo "[*] Get fuse2 ... "
    prefix=${prefixroot}/fuse2
    sudo rm -rf $prefix || true
    mkdir -p $prefix
    zipname=fuse-2.9.7
    if [[ ! -f ${pkgroot}/${zipname}.zip ]]
    then
        cd $pkgroot && wget https://github.com/libfuse/libfuse/archive/fuse-2.9.7.zip -O ${pkgroot}/${zipname}.zip
    fi
    cd $pkgroot && unzip ${zipname}.zip  && cd libfuse-${zipname}
    ./makeconf.sh
    ./configure --prefix=${prefix} --exec-prefix=${prefix}
    make -j8
    sudo make install
    cd && rm -rf ${pkgroot}/libfuse-${zipname}
    touch ${prefixroot}/FUSE2
    echo "export PATH=${prefix}/bin:\$PATH" >> ${HOME}/.bashrc
    echo "fuse is done!"
fi

if [[ ! -f ${prefixroot}/CMAKE ]]
then
    echo "[*] Get cmake ..."
    prefix=${prefixroot}/cmake
    sudo rm -rf ${prefix} || true
    mkdir -p ${prefix}
    pkgname=cmake-3.12.0-rc3-Linux-x86_64
    if [[ ! -f ${pkgroot}/${pkgname}.tar.gz ]]
    then
       cd ${pkgroot} && wget https://cmake.org/files/v3.12/${pkgname}.tar.gz -O ${pkgname}.tar.gz
    fi
    cd ${pkgroot} && tar -zxvf ${pkgname}.tar.gz
    cp --verbose -RT ${pkgname} ${prefix}
    cd && rm -rf ${pkgroot}/{pkgname}
    touch ${prefixroot}/CMAKE
    echo "export PATH=${prefix}/bin:\$PATH" >> ${HOME}/.bashrc
    echo "cmake is done!"
fi
