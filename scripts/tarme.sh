#! /bin/bash
# Author: bugnofree
# Date: 2018-09-12
# Contact: pwnkeeper@gmail.com

cd $(realpath $(dirname "$0")) && cd ..

tarname=rlfs2
if [[ -d ${tarname} ]]
then
    rm -rf ${tarname}
fi
mkdir -p ${tarname}
cp -a CMakeLists.txt README.md bbfs scripts src ${tarname}
tar zcvf ${tarname}.tgz ${tarname} &> /dev/null
rm -rf ${tarname}

echo "[+] ${tarname}.tgz is generated!"
