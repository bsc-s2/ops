#!/bin/sh

source ../shlib.sh

yum_repo_add()
{
    curl -L https://copr.fedorainfracloud.org/coprs/mcepl/vim8/repo/epel-7/mcepl-vim8-epel-7.repo \
        -o /etc/yum.repos.d/mcepl-vim8-epel-7.repo
}


yum_remove()
{
    for pkg in "$@"; do
        yum remove -y $pkg || die yum remove $pkg
    done
}

yum_install()
{
    for pkg in "$@"; do
        yum install -y $pkg || die yum install $pkg
    done
}

yum_pkg_unused()
{
    echo 'puppet*'
}

yum_pkg_util()
{
    echo git-all
    echo graphviz           # draw graph `dot`
    echo gunplot            # chart/plotting tool
    echo mercurial          # version control `hg`
    echo vim-enhanced

    echo cloc               # line of code analysis
    echo lsof
    echo net-tools
    echo openssl
    echo parallel
    echo psmisc             # killall
    echo tcpdump
    echo telnet
    echo tree
    echo unzip

    echo screen
    echo tmux

    echo man
    echo man-db
    echo man-pages

    # for nginx test
    echo perl
    echo perl-ExtUtils-*

    echo w3m                # console web browser
    echo wget
}

yum_pkg_dev()
{
    echo gcc
    echo ctags-etags
    echo perf
    echo openssl-devel
    echo libcurl-devel

    echo perl-devel

    echo file-devel # magic.h

    echo uuid
    echo libuuid
    echo libuuid-devel

    echo luarocks

    echo gperftools-libs # tcmalloc
}

main()
{
    info start init centos-7

    yum_repo_add                 || die yum_repo_add
    yum_remove $(yum_pkg_unused) || die yum_remove
    yum update -y                || die yum update
    yum_install $(yum_pkg_util)  || die yum_install util
    yum_install $(yum_pkg_dev)   || die yum_install dev
}

main "$@"
