#!/bin/sh

main()
{
    info start init centos-7 as a minimal dev env...

    yum_repo_add                       || die yum_repo_add
    yum_remove       $(yum_pkg_unused) || die yum_remove
    yum update -y                      || die yum update
    yum_install      $(yum_pkg_util)   || die yum_install util
    yum_install      $(yum_pkg_dev)    || die yum_install dev
    pipconf_install                    || die pipconf_install
    pip2_install     $(pip_pkg_all)    || die pip2_install all
    make_ssh_key                       || die make_ssh_key
    screenrc_install                   || die screenrc_install
    tmuxconf_install                   || die tmuxconf_install
    bashrc_install                     || die bashrc_install
    vimrc_install                      || die vimrc_install

    ok Done init centos-7 as a minimal dev env
}

yum_repo_add()
{
    curl -L https://copr.fedorainfracloud.org/coprs/mcepl/vim8/repo/epel-7/mcepl-vim8-epel-7.repo \
        -o /etc/yum.repos.d/mcepl-vim8-epel-7.repo
}

yum_remove()
{
    for pkg in "$@"; do
        info "yum remove $pkg"
        yum remove -y $pkg || die yum remove $pkg
    done
}

yum_install()
{
    for pkg in "$@"; do
        info "yum install $pkg"
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
    echo gnuplot            # chart/plotting tool
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
[global]
index-url = http://mirrors.aliyun.com/pypi/simple/
[install]
trusted-host=mirrors.aliyun.com
END
    } > "$HOME/.pip/pip.conf"
}

pip2_install()
{
    info "pip2 upgrape pip"
    pip2 install --upgrade pip

    for pkg in "$@"; do
        info "pip2 install $pkg"
        pip2 install $pkg || die pip install $pkg
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
# usage: copy to ~/.screenrc

# use Ctrl-e as escape key, by default it is Ctrl-a
# escape ^e^e

# change the hardstatus settings to give an window list at the bottom of the
# screen, with the time and date and with the current window highlighted

# statusline: [ drdrxpdeMacBook-Pro ][              0 bash  1- bash  (2*bash)              ][ 21/06 14:04 ]

hardstatus alwayslastline
hardstatus string '%{= kG} %{= kw}%?%-Lw%?%{r}(%{W}%n*%f%t%?(%u)%?%{r})%{w}%?%+Lw%?%?%= %d/%m %c'

# 10k lines of scrollback buffer
defscrollback 10000

# <F8> switch to next window, <F7> to prev window
bindkey -k k8 next
bindkey -k k7 prev

# To switch to previous or next window
# alt-h
# alt-l
# \033 is <ESC>
bindkey -t "\033h" prev
bindkey -t "\033l" next

# To move current window to left or right
# alt-shift-h
# alt-shift-l
bindkey -t "\033H" exec sh -c 'screen -X msgwait 0; n=$(screen -Q number | grep -o "^[0-9]*"); let n=n-1; screen -X number "$n"; screen -X msgwait 1;'
bindkey -t "\033L" exec sh -c 'screen -X msgwait 0; n=$(screen -Q number | grep -o "^[0-9]*"); let n=n+1; screen -X number "$n"; screen -X msgwait 1;'

# ctrl-a ctrl-r to reload .screenrc
# bind uses c-a, bindkey does not
bind "^r" source ~/.screenrc

# display error message for 1 second
msgwait 1

# turn off stupid startup message
startup_message off
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
# Change default prefix key
# set-option -g prefix C-a

set-option        -g base-index               1
set               -g default-terminal         "xterm-256color"
set               -g display-time             3000
set               -g history-limit            65535
set-option        -g status-keys              vi
set-option        -g status-right             "#(date +%H:%M' ')" # 状态栏右方的内容；这里的设置将得到类似23:59的显示
set-option        -g status-right-length      10                  # 状态栏右方的内容长度；建议把更多的空间留给状态栏左方（用于列出当前窗口）
set-window-option -g window-status-current-bg yellow              # Highlight Current Window

#此类设置可以在命令行模式中输入show-window-options -g查询

set-window-option -g mode-keys vi    #复制模式中的默认键盘布局；可以设置为vi或emacs

#bind-key -t vi-copy 'C-v' rectangle-toggle # Begin selection in copy mode.

# split panes using | and -

bind | split-window -h
bind - split-window -v

# switch panes using Alt-arrow without prefix

bind l select-pane -L
bind h select-pane -R
bind k select-pane -U
bind j select-pane -D
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
# User specific aliases and functions

alias rm='rm -i'
alias cp='cp -i'
alias mv='mv -i'

LANG="zh_CN.UTF-8"
LC_ALL="zh_CN.UTF-8"

# Source global definitions
if [ -f /etc/bashrc ]; then
    . /etc/bashrc
fi

os_detect()
{
    case $(uname -s) in
        Linux)
            os=linux ;;
        *[bB][sS][dD])
            os=bsd ;;
        Darwin)
            os=mac ;;
        *)
            os=unix ;;
    esac
    echo $os
}

ps_pwd()
{
    local LightBlue="$(tput bold ; tput setaf 4)"
    local NC="$(tput sgr0)" # No Color
    local height=$(tput lines)

    local cwd="${PWD#$HOME}"
    if test ".$cwd" != ".$PWD"; then
        cwd="~$cwd"
    fi
    test "$height" -gt 10 \
        && echo "
$LightBlue$cwd$NC" \
        || echo " $LightBlue$cwd$NC"
}
init_prompt()
{
    Red="$(tput setaf 1)"
    Green="$(tput setaf 2)"
    LightGreen="$(tput bold ; tput setaf 2)"
    Brown="$(tput setaf 3)"
    Yellow="$(tput bold ; tput setaf 3)"
    LightBlue="$(tput bold ; tput setaf 4)"
    NC="$(tput sgr0)" # No Color

    local ps="$Green\u \h$NC"
    ps=$ps"$Yellow${eth0_ip}$NC"
    ps=$ps".\j"

    # current git branch
    ps=$ps" $LightGreen\$(git branch --no-color 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/(\1)/')$NC"

    # tracking remote branc
    ps=$ps" -> $Red\$(git config --get branch.\$(git symbolic-ref --short HEAD 2>/dev/null).remote 2>/dev/null)/$NC"
    ps=$ps"$Brown\$(git config --get branch.\$(git symbolic-ref --short HEAD 2>/dev/null).merge 2>/dev/null | sed 's/^refs\/heads\///')$NC"
    ps=$ps" \t:"
    ps=$ps"\$(ps_pwd 2>/dev/null || { echo; pwd; })\n"
    ps=$ps"☛ "
    export PS1="$ps"
}
os=$(os_detect)
if [ "$os" = "linux" ] ; then
    eth0_ip=`echo $(/sbin/ifconfig  | grep 'inet \(addr:\)\?'| grep -v '127.0.0.1' |  grep -v 'Mask:255.255.255.255' | awk '{gsub("addr:","",$2); print " <"$2">"}')`
elif [ "$os" = "bsd" ]; then
    eth0_ip=`/sbin/ifconfig | grep "inet"|awk -F "." '{print $3"."$4}'| awk '{print $2}' | head -n 3`
elif [ "$os" == "mac" ]; then
    eth0_ip=`ifconfig  | grep '\binet\b' | grep -v '127.0.0.1' | awk '/inet /{i = i " <" substr($2, 1) ">"} END{print i}'`
else
    eth0_ip="unknown"
fi
init_prompt
END
    } >"$HOME/.bashrc"

    ls -l "$HOME/.bashrc"
}

vimrc_install()
{
    info "install $HOME/.vimrc"

    if [ -f "$HOME/.vimrc" ]; then
        return 0
    fi

    # source is replace by Makefile

    {
    cat<<-'END'
END
    } >"$HOME/.vimrc"

    # TODO init plugins
}

#!/bin/sh


shlib_init_colors()
{
    Black="$(                   tput setaf 0)"
    BlackBG="$(                 tput setab 0)"
    DarkGrey="$(     tput bold; tput setaf 0)"
    LightGrey="$(               tput setaf 7)"
    LightGreyBG="$(             tput setab 7)"
    White="$(        tput bold; tput setaf 7)"
    Red="$(                     tput setaf 1)"
    RedBG="$(                   tput setab 1)"
    LightRed="$(     tput bold; tput setaf 1)"
    Green="$(                   tput setaf 2)"
    GreenBG="$(                 tput setab 2)"
    LightGreen="$(   tput bold; tput setaf 2)"
    Brown="$(                   tput setaf 3)"
    BrownBG="$(                 tput setab 3)"
    Yellow="$(       tput bold; tput setaf 3)"
    Blue="$(                    tput setaf 4)"
    BlueBG="$(                  tput setab 4)"
    LightBlue="$(    tput bold; tput setaf 4)"
    Purple="$(                  tput setaf 5)"
    PurpleBG="$(                tput setab 5)"
    Pink="$(         tput bold; tput setaf 5)"
    Cyan="$(                    tput setaf 6)"
    CyanBG="$(                  tput setab 6)"
    LightCyan="$(    tput bold; tput setaf 6)"
    NC="$(                      tput sgr0)" # No Color
}

screen_width()
{
    local chr="${1--}"
    chr="${chr:0:1}"

    local width=$(tput cols 2||echo 80)
    width="${COLUMNS:-$width}"

    echo $width
}

hr()
{
    # generate a full screen width horizontal ruler
    local width=$(screen_width)

    printf -vl "%${width}s\n" && echo ${l// /$chr};
}

remove_color()
{
    # remove color control chars from stdin or first argument

    local sed=gsed
    which -s $sed || sed=sed

    local s="$1"
    if [ -z "$s" ]; then
        $sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g"
    else
        echo "$s" | remove_color
    fi

}

text_hr()
{
    # generate a full screen width sperator line with text.
    # text_hr "-" "a title"
    # > a title -----------------------------------------
    #
    # variable LR=l|m|r controls alignment

    local chr="$1"
    shift

    local bb="$(echo "$@" | remove_color)"
    local text_len=${#bb}

    local width=$(screen_width)
    let width=width-text_len

    local lr=${LR-m}
    case $lr in
        m)
            let left=width/2
            let right=width-left
            echo "$(printf -vl "%${left}s\n" && echo ${l// /$chr})$@$(printf -vl "%${right}s\n" && echo ${l// /$chr})"
            ;;
        r)

            echo "$(printf -vl "%${width}s\n" && echo ${l// /$chr})$@"
            ;;
        *)
            # l by default
            echo "$@$(printf -vl "%${width}s\n" && echo ${l// /$chr})"
            ;;
    esac

}



SHLIB_LOG_VERBOSE=1
SHLIB_LOG_FORMAT='[$(date +"%Y-%m-%d %H:%M:%S")] $level $title $mes'

die()
{
    err "$@" >&2
    exit 1
}
die_empty()
{
    if test -z "$1"
    then
        shift
        die empty: "$@"
    fi
}

set_verbose()
{
    SHLIB_LOG_VERBOSE=${1-1}
}

log()
{
    local color="$1"
    local title="$2"
    local level="$_LOG_LEVEL"
    shift
    shift

    local mes="$@"
    local NC="$(tput sgr0)"

    if [ -t 1 ]; then
        title="${color}${title}${NC}"
        level="${color}${level}${NC}"
    fi
    eval "echo \"$SHLIB_LOG_FORMAT\""
}
dd()
{
    debug "$@"
}
debug()
{
    if [ ".$SHLIB_LOG_VERBOSE" = ".1" ]; then
        local LightCyan="$(tput bold ; tput setaf 6)"
        _LOG_LEVEL=DEBUG log "$LightCyan" "$@"
    fi
}
info()
{
    local Brown="$(tput setaf 3)"
    _LOG_LEVEL=" INFO" log "$Brown" "$@"
}
ok() {
    local Green="$(tput setaf 2)"
    _LOG_LEVEL="   OK" log "${Green}" "$@"
}
err() {
    local Red="$(tput setaf 1)"
    _LOG_LEVEL="ERROR" log "${Red}" "$@"
}

git_hash()
{
    git rev-parse $1 \
        || die "'git_hash $@'"
}
git_is_merge()
{
    test $(git cat-file -p "$1" | grep "^parent " | wc -l) -gt 1
}
git_parents()
{
    git rev-list --parents -n 1 ${1-HEAD} | { read self parents; echo $parents; }
}
git_rev_list()
{
    # --parents
    # print parent in this form:
    #     <commit> <parent-1> <parent-2> ..

    git rev-list \
        --reverse \
        --topo-order \
        --default HEAD \
        --simplify-merges \
        "$@" \
        || die "'git rev-list $@'"
}
git_tree_hash()
{
    git rev-parse "$1^{tree}"
}
git_ver()
{
    local git_version=$(git --version | awk '{print $NF}')
    local git_version_1=${git_version%%.*}
    local git_version_2=${git_version#*.}
    git_version_2=${git_version_2%%.*}

    printf "%03d%03d" $git_version_1 $git_version_2
}
git_working_root()
{
    git rev-parse --show-toplevel
}

git_rev_exist()
{
    git rev-parse --verify --quiet "$1" >/dev/null
}

git_branch_default_remote()
{
    local branchname=$1
    git config --get branch.${branchname}.remote
}
git_branch_default_upstream_ref()
{
    local branchname=$1
    git config --get branch.${branchname}.merge
}
git_branch_default_upstream()
{
    git rev-parse --abbrev-ref --symbolic-full-name "$1"@{upstream}

    # OR
    # git_branch_default_upstream_ref "$@" | sed 's/^refs\/heads\///'
}
git_branch_exist()
{
    git_rev_exist "refs/heads/$1"
}

git_head_branch()
{
    git symbolic-ref --short HEAD
}

git_commit_date()
{

    # git_commit_date author|commit <ref> [date-format]

    # by default output author-date
    local what_date="%ad"
    if [ "$1" = "commit" ]; then
        # commit date instead of author date
        what_date="%cd"
    fi
    shift

    local ref=$1
    shift

    local fmt="%Y-%m-%d %H:%M:%S"
    if [ "$#" -gt 0 ]; then
        fmt="$1"
    fi
    shift

    git log -n1 --format="$what_date" --date=format:"$fmt" "$ref"
}
git_commit_copy()
{
    # We're going to set some environment vars here, so
    # do it in a subshell to get rid of them safely later
    dd copy_commit "{$1}" "{$2}" "{$3}"
    git log -1 --pretty=format:'%an%n%ae%n%ad%n%cn%n%ce%n%cd%n%s%n%n%b' "$1" |
    (
    read GIT_AUTHOR_NAME
    read GIT_AUTHOR_EMAIL
    read GIT_AUTHOR_DATE
    read GIT_COMMITTER_NAME
    read GIT_COMMITTER_EMAIL
    read GIT_COMMITTER_DATE
    export  GIT_AUTHOR_NAME \
        GIT_AUTHOR_EMAIL \
        GIT_AUTHOR_DATE \
        GIT_COMMITTER_NAME \
        GIT_COMMITTER_EMAIL \
        GIT_COMMITTER_DATE

    # (echo -n "$annotate"; cat ) |

    git commit-tree "$2" $3  # reads the rest of stdin
    ) || die "Can't copy commit $1"
}

git_object_type()
{
    # $0 ref|hash
    # output "commit", "tree" etc
    git cat-file -t "$@" 2>/dev/null
}
git_object_add_by_commit_path()
{
    # add an blob or tree object to target_path in index
    # the object to add is specified by commit and path
    local target_path="$1"
    local src_commit="$2"
    local src_path="$3"

    local src_dir="$(dirname "$src_path")/"
    local src_name="$(basename "$src_path")"
    local src_treeish="$(git rev-parse "$src_commit:$src_dir")"

    git_object_add_by_tree_name "$target_path" "$src_treeish" "$src_name"

}
git_object_add_by_tree_name()
{
    # add an blob or tree object to target_path in index
    local target_path="$1"
    local src_treeish="$2"
    local src_name="$3"

    dd "arg: target_path: ($target_path) src_treeish: ($src_treeish) src_name: ($src_name)"

    local target_dir="$(dirname $target_path)/"
    local target_fn="$(basename $target_path)"
    local treeish

    if [ -z "$src_name" ] || [ "$src_name" = "." ] || [ "$src_name" = "./" ]; then
        treeish="$src_treeish"
    else
        treeish=$(git ls-tree "$src_treeish" "$src_name" | awk '{print $3}')
    fi

    dd "hash of object to add is: $treeish"

    if [ "$(git_object_type $treeish)" = "blob" ]; then
        # the treeish imported is a file, not a dir
        # first create a wrapper tree or replace its containing tree

        dd "object to add is blob"

        local dir_treeish
        local target_dir_treeish="$(git rev-parse "HEAD:$target_dir")"
        if [ -n "target_dir_treeish" ]; then
            dir_treeish="$(git rev-parse "HEAD:$target_dir")"
            dd "target dir presents: $target_dir"

        else
            dd "target dir absent"
            dir_treeish=""
        fi

        treeish=$(git_tree_add_blob "$dir_treeish" "$target_fn" $src_treeish $src_name) || die create wrapper treeish
        target_path="$target_dir"

        dd "wrapper treeish: $treeish"
        dd "target_path set to: $target_path"
    else
        dd "object to add is tree"
    fi

    git_treeish_add_to_prefix "$target_path" "$treeish"
}

git_treeish_add_to_prefix()
{
    local target_path="$1"
    local treeish="$2"

    dd treeish content:
    git ls-tree $treeish

    git rm "$target_path" -r --cached || dd removing target "$target_path"

    if [ "$target_path" = "./" ]; then
        git read-tree "$treeish" \
            || die "read-tree $target_path $treeish"
    else
        git read-tree --prefix="$target_path" "$treeish" \
            || die "read-tree $target_path $treeish"
    fi
}

git_tree_add_tree()
{
    # output new tree hash in stdout
    # treeish can be empty
    local treeish="$1"
    local target_fn="$2"
    local item_hash="$3"
    local item_name="$4"

    {
        if [ -n "$treeish" ]; then
            git ls-tree "$treeish" \
                | fgrep -v "	$item_name"
        fi

        cat "040000 tree $item_hash	$target_fn"
    } | git mktree
}
git_tree_add_blob()
{
    # output new tree hash in stdout
    # treeish can be empty
    local treeish="$1"
    local target_fn="$2"
    local blob_treeish="$3"
    local blob_name="$4"

    {
        if [ -n "$treeish" ]; then
            git ls-tree "$treeish" \
                | fgrep -v "	$target_fn"
        fi

        git ls-tree "$blob_treeish" "$blob_name" \
            | awk -v target_fn="$target_fn" -F"	" '{print $1"	"target_fn}'
    } | git mktree
}

git_copy_commit()
{
    git_commit_copy "$@"
}

git_diff_ln_new()
{
    # output changed line number of a file: <from> <end>; inclusive:
    # 27 28
    # 307 309
    # 350 350
    #
    # Usage:
    #
    #   diff working tree with HEAD:
    #       git_diff_ln_new HEAD -- <fn>
    #
    #   diff working tree with staged:
    #       git_diff_ln_new -- <fn>
    #
    #   diff staged(cached) with HEAD:
    #       git_diff_ln_new --cached -- <fn>
    #
    # in git-diff output:
    # for add lines:
    # @@ -53 +72,8
    #
    # for remove lines:
    # @@ -155 +179,0

    git diff -U0 "$@" \
        | grep '^@@' \
        | awk '{

    # @@ -155 +179,0
    # $1 $2   $3

    l = $3
    gsub("^+", "", l)

    # add default offset: ",1"
    split(l",1", x, ",")

    # inclusive line range:
    x[2] = x[1] + x[2] - 1

    # line remove format: @@ -155, +179,0
    # do need to output line range for removed.
    if (x[2] >= x[1]) {
        print x[1] " " x[2]
    }

}'
}

# test:

# # file to root
# git reset --hard HEAD
# git_object_add_by_commit_path a HEAD dist/shlib.sh
# git status
# # dir to root
# git reset --hard HEAD
# git_object_add_by_commit_path a HEAD dist
# git status
# # file to folder
# git reset --hard HEAD
# git_object_add_by_commit_path a/b/c HEAD dist/shlib.sh
# git status
# # dir to folder
# git reset --hard HEAD
# git_object_add_by_commit_path a/b/c HEAD dist
# git status


os_detect()
{
    local os
    case $(uname -s) in
        Linux)
            os=linux ;;
        *[bB][sS][dD])
            os=bsd ;;
        Darwin)
            os=mac ;;
        *)
            os=unix ;;
    esac
    echo $os
}

mac_ac_power_connection()
{
    #  Connected: (Yes|No)
    system_profiler SPPowerDataType \
        | sed '1,/^ *AC Charger Information:/d' \
        | grep Connected:
}


mac_power()
{

    # $0 is-battery          exit code 0 if using battery.
    # $0 is-ac-power         exit code 0 if using ac power.

    local cmd="$1"
    local os=$(os_detect)

    if [ "$os" != "mac" ]; then
        err "not mac but: $os"
        return 1
    fi

    case $cmd in

        is-battery)
            mac_ac_power_connection | grep -q No
            ;;

        is-ac-power)
            mac_ac_power_connection | grep -q Yes
            ;;

        *)
            err "invalid cmd: $cmd"
            return 1
            ;;
    esac
}

fn_match()
{
    # $0 a.txt *.txt
    case "$1" in
        $2)
            return 0
            ;;
    esac
    return 1
}

main "$@"
