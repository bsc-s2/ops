#!/bin/sh

main()
{
    info start init centos-7 as a minimal dev env...

    yum_repo_add                       || die yum_repo_add
    yum_remove       $(yum_pkg_unused) || die yum_remove
    sudo yum update -y                 || die yum update
    yum_install      $(yum_pkg_util)   || die yum_install util
    yum_install      $(yum_pkg_dev)    || die yum_install dev
    pipconf_install                    || die pipconf_install
    pip2_install     $(pip_pkg_all)    || die pip2_install all
    make_ssh_key                       || die make_ssh_key
    screenrc_install                   || die screenrc_install
    tmuxconf_install                   || die tmuxconf_install
    bashrc_install                     || die bashrc_install
    vimrc_install                      || die vimrc_install
    vim_plugin_install                 || die vim_plugin_install

    ok Done init centos-7 as a minimal dev env
}

yum_repo_add()
{
    sudo curl -L https://copr.fedorainfracloud.org/coprs/mcepl/vim8/repo/epel-7/mcepl-vim8-epel-7.repo \
        -o /etc/yum.repos.d/mcepl-vim8-epel-7.repo
}

yum_remove()
{
    for pkg in "$@"; do
        info "yum remove $pkg"
        sudo yum remove -y $pkg || die yum remove $pkg
    done
}

yum_install()
{
    for pkg in "$@"; do
        info "yum install $pkg"
        sudo yum install -y $pkg || die yum install $pkg
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
    echo gnuplot            # chart/plotting tool
    echo mercurial          # version control `hg`
    echo vim-enhanced

    echo lsof
    echo net-tools
    echo openssl
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

    echo mysql-devel # for MySQL-python

    echo python2-pip
    echo python-devel # for building ipython

    echo perl-devel

    echo file-devel # magic.h

    echo uuid
    echo libuuid
    echo libuuid-devel

    echo luarocks

    echo gperftools-libs # tcmalloc
}

pipconf_install()
{
    if [ -f "$HOME/.pip/pip.conf" ]; then
        return 0
    fi

    mkdir "$HOME/.pip"

    {
        cat <<-'END'
source pip.conf
END
    } > "$HOME/.pip/pip.conf"
}

pip2_install()
{
    info "pip2 upgrape pip"
    sudo pip2 install --upgrade pip

    for pkg in "$@"; do
        info "pip2 install $pkg"
        sudo pip2 install $pkg || die pip install $pkg
    done
}

pip_pkg_all()
{
    # interactive python

    echo ipython==5.7.0    # the latest ipython that supports python2.7 

    echo googlecl
    echo pipdeptree
    echo virtualenv

    echo json2yaml
    echo pypinyin        # convert 汉子 to pinyin

    echo MySQL-python

    echo pygtrie       # trie impl by google

    echo requests
    echo urllib3
    echo scrapy
    echo tinycss
    echo markdown2
    echo netifaces
    echo psutil            # get cpu, memory, network stats
    echo glances           # An eye on your system
    echo bottle            # web framework for glances
    echo dbx-stopwatch     # profiler
    echo mysql-replication # import pymysqlreplication
    echo subprocess32      # replacement of subprocess, fixed Popen race-condition: http://drmingdrmer.github.io/tech/programming/2017/11/20/python-concurrent-popen.html

    # debugging

    echo objgraph
    echo pyrasite  # attach to existing python program
    echo guppy     # memory examine
    echo snakefood # dependency graph

    echo jedi     # Awesome autocompletion and static analysis library for python

    echo GitPython
    echo gitconfig
    echo shadowsocks

    # code lint / format

    echo pyflakes
    echo flake8
    echo autoflake
    echo autopep8
    echo isort
    echo yapf    # code formatter
    echo vulture # find unused function and variables

    echo unp    # unpack anythin
    echo pydf   # python version of linux `df`
    echo ici    # dictionary
}

make_ssh_key() {
    info "make_ssh_key: $HOME/.ssh/id_rsa" \
        && { [ -f "$HOME/.ssh/id_rsa" ] && info "exists" && return 0 || ``; } \
        && mkdir -p ~/.ssh \
        && ssh-keygen -f ~/.ssh/id_rsa -N "" \
        && cat ~/.ssh/id_rsa.pub \
        && info "Done make_ssh_key"
}

screenrc_install()
{
    info "install $HOME/.screenrc"

    if [ -f "$HOME/.screenrc" ]; then
        return 0
    fi

    # source is replace by Makefile

    {
    cat<<-'END'
source screenrc
END
    } >"$HOME/.screenrc"

    ls -l "$HOME/.screenrc"
}

tmuxconf_install()
{
    info "install $HOME/.tmux.conf"

    if [ -f "$HOME/.tmux.conf" ]; then
        return 0
    fi

    # source is replace by Makefile

    {
    cat<<-'END'
source tmux.conf
END
    } >"$HOME/.tmux.conf"

    ls -l "$HOME/.tmux.conf"
}

bashrc_install()
{
    info "install $HOME/.bashrc"

    if [ -f "$HOME/.bashrc" ] && grep -q 's2-dev-env' "$HOME/.bashrc"; then
        return 0
    fi

    # source is replace by Makefile

    {
    cat<<-'END'
# s2-dev-env auto generated
source bashrc
END
    } >"$HOME/.bashrc"

    ls -l "$HOME/.bashrc"
}

vimrc_install()
{
    info "install $HOME/.vimrc"

    if [ -f "$HOME/.vimrc" ] && grep -q 's2-dev-env' "$HOME/.vimrc"; then
        return 0
    fi

    # source is replace by Makefile

    {
    cat<<-'END'
" s2-dev-env auto generated
source vimrc
END
    } >"$HOME/.vimrc"
}

vim_plugin_install()
{
    info "install vim plugins with BundleInstall"

    vundle_path="$HOME/.vim/bundle"
    if [ ! -d "$vundle_path/Vundle.vim" ]; then
        mkdir -p "$vundle_path" || die make dir "$vundle_path"

        git clone https://github.com/VundleVim/Vundle.vim.git \
            $vundle_path/Vundle.vim \
            || die "clone $vundle_path/Vundle.vim"
    fi

    vim +BundleInstall +qall - </dev/null
}

source ../shlib.sh

main "$@"
