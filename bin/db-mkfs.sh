#!/bin/sh

echo "===== cmd ($0) ($1) ($2) ($3)"
echo "===== env MKFS_FORCE: ($MKFS_FORCE)"

flags=
if [ "$MKFS_FORCE" = '1' ]; then
    flags="$flags -f"
fi

dev=$1
should_gt_in_gb=$2

usage()
{
    echo "make xfs on /dev/sdb and requires its size >= 1200 GB:"
    echo "  $0 [-f] /dev/sdb 1200"
    echo ""
    echo "force mkfs on a device already with a FS:"
    echo "  MKFS_FORCE=1 $0 [-f] /dev/sdb 1200"
    echo ""
    echo "run from remote:"
    echo "  curl https://coding.net/u/drmingdrmer/p/ops/git/raw/master/bin/db-mkfs.sh | sh -s /dev/sdb 12000"
}

if [ ! -b $dev ]; then
    echo ===== not a block device: $dev
    usage
    exit 1
fi

if [ -z "$should_gt_in_gb" ]; then
    usage
    exit 1
fi

label=/data1
dir=$label

echo ===== make db partition $dir for db from $dev
echo ===== press enter to continue
read -n 1

mkdir -p $dir \
    && mkfs.xfs $flags $dev \
    && xfs_admin -L $label $dev \
    || exit 1

cat >>/etc/fstab <<-END
LABEL=$label  $dir  xfs  noatime,nodiratime,nobarrier,logbufs=8  0  0
END

echo ===== fstab is:
cat /etc/fstab
n=$(grep "$dir" /etc/fstab | wc -l)
if [ $n -gt 1 ]; then
    echo ===== multiple lines in /etc/fstab for "$dir"
    echo ===== you might need to clean it up.
fi

echo ===== mount all
mount -a \
    || exit 1

df -h $dir

GB=$(df -m $dir | tail -n1 | awk '{print int($2 / 1024)}')
if [ $GB -gt $should_gt_in_gb ]; then
    echo ===== mounted size is: $GB GB
else
    echo '===== size in GB is not correct: expect:' $should_gt_in_gb 'actual:' $GB
    exit 1
fi
