#!/bin/bash

test_name="tmeta_with_shared_internals_and_leaves"
metadata_dump="${test_name}.bin"
metadata_pack="${test_name}.pack"

vg=dmtest
tp=tp1
pool_size=8g
metadata_size=4m
blocksize=64k
lv_name=lv1
lv_size=5g
snap_name=snap1

lvcreate ${vg} --type thin-pool --name ${tp} --size ${pool_size} \
         --chunksize ${blocksize} --poolmetadatasize ${metadata_size} \
         -Zn --poolmetadataspare=n

lvcreate ${vg} --type thin --name ${lv_name} --thinpool ${tp} --virtualsize ${lv_size}

# commit a metadata transaction
dmsetup status "${vg}-${tp}-tpool"

# write sufficient amount of data to produce a mapping tree with height == 2
dd if=/dev/zero of="/dev/mapper/${vg}-${lv_name}" bs=1M count=4096

# create a snapshot
lvcreate "${vg}/${lv_name}" --snapshot --name ${snap_name} -kn

# append some data to shadow the last internal node
dd if=/dev/zero of="/dev/mapper/${vg}-${snap_name}" bs=1M count=4 seek=4096

lvchange -an "${vg}/${lv_name}"
lvchange -an "${vg}/${snap_name}"
lvchange -an "${vg}/${tp}"

# dump metadata
lvchange -an "${vg}"
lvchange -ay "${vg}/${tp}_tmeta" -f -y
dd if="/dev/mapper/${vg}-${tp}_tmeta" of="${metadata_dump}" oflag=direct
lvchange -an "${vg}/${tp}_tmeta"

../../../target/release/pdata_tools thin_metadata_pack -i "${metadata_dump}" -o "../${metadata_pack}"

rm ${metadata_dump}

lvremove "${vg}/${snap_name}"
lvremove "${vg}/${lv_name}"
lvremove "${vg}/${tp}"
