#!/bin/sh

gindex=0

step()
{
    gindex=$(expr ${gindex} + 1)
    echo -e "\033[36m[step ${gindex}]: $1\033[0m"
}

info()
{
    echo -e "    \033[35m[Info] $1\033[0m"
    return 0
}

ok()
{
    echo -e "    \033[32m[OK] $1\033[0m"
    return 0
}

warn()
{
    echo -e "    \033[33m\033[01m\033[05m[Warn] $1\033[0m"
    return 1
}

error()
{
    echo -e "    \033[31m\033[01m[Error] $1\033[0m"
    return 2
}

type=$1

server_types=('storage-6t' 'storage-8t' 'front' 'ssd')

if [[ ${type} == "s6t" ]];then
    type="storage-6t"
elif [[ ${type} == "s8t" ]];then
    type='storage-8t'
fi

if [[ -z $type ]];then
    error "No server type specified"
    info "Usage: $0 <server_type>"
    info "<server_type> mybe 'storage-6t(s6t)','storage-8t(s8t)','front','ssd'"
    exit
fi


if [[ ! $(echo ${server_types[*]} | grep -w ${type}) ]];then
    error "Invalid server_type"
    info "<server_type> mybe 'storage-6t(s6t)','storage-8t(s8t)','front','ssd'"
    exit
fi

stable_os_release=('7.3.1611' '7.2.1511')

_install_package()
{
    packages_name=$1

    for pname in ${packages_name};do

        if [ ! -f /usr/bin/${pname} ];then
            yum -y install ${pname} &> /dev/null
            if  [ $? == 0 ];then
                ok "yum install ${pname} successfully"
            else
                error "yum install ${pname} failed"
            fi
        else
            ok "${pname} have installed"
        fi

    done
}

install_package()
{
    step "install package"

    _install_package "wget unzip vim dstat git lrzsz net-tools bind-utils ipmitool"

    if [[ ! -f /usr/local/bin/sas3ircu ]];then
        wget -q  http://s2.i.qingcdn.com/s2-package/sas3ircu -O /usr/local/bin/sas3ircu

        if [[ $? == 0 ]];then
            ok "install sas3ircu successfully"
        else
            error "install sas3ircu failed"
        fi

        chmod +x /usr/local/bin/sas3ircu
    else
        ok "sas3ircu have installed"
    fi

    if [[ ! -f /opt/MegaRAID/MegaCli/MegaCli64 ]];then

        if [[ ! -f /tmp/Linux_MegaCLI-8-07-07.zip ]];then
            #info "wget Linux_MegaCLI-8-07-07.zip from https://docs.broadcom.com"
            wget -q https://docs.broadcom.com/docs-and-downloads/sep/oracle/files/Linux_MegaCLI-8-07-07.zip -O /tmp/Linux_MegaCLI-8-07-07.zip
        fi

        unzip -qqo /tmp/Linux_MegaCLI-8-07-07.zip -d /tmp/

        rpm  --quiet -i  /tmp/MegaCli-8.07.07-1.noarch.rpm &> /dev/null

        if [[ $? == 0 ]]; then
            ok "rpm install megacli successfully"
        else
            error "rpm install megacli failed"
        fi

    else
       ok "megacli have installed"
    fi
}

check_os_release()
{
    step "check os release"
    curr_os_release=$(cat /etc/redhat-release | awk '{print $4}')

    for sor in ${stable_os_release[*]};do
        if [ ${curr_os_release} == ${sor} ];then
            ok "current os release is ${curr_os_release}"
            return 0
        fi
    done

    error "os release ${curr_os_release}, not CentOS 7.2.1511 or 7.3.1611"
    return 1
}

check_network()
{
    step "check public network "
    ping -c 3 -i 0.2 -W 3 114.114.114.114 &> /dev/null

    if [ $? == 0 ];then
        ok "network is ok, let's go to init s2 servers......."
    else
        error "ping 114.114.114.114 failed !!!"
        return 1
    fi

    step "check resolve a domain name"
    curl baidu.com >/dev/null 2>&1

    if [[ $? == 0 ]];then
        ok "resolve a domain successfully"
    else
        warn "test failed, mybe bad resolve config, reset nameserver 8.8.8.8"
        echo "nameserver 8.8.8.8" >> /etc/resolv.conf

        step "check resolve a domain name again"

        curl baidu.com >/dev/null 2>&1

        if [[ $? == 0 ]];then
            ok "reslve a domain successfully"
        else
            error "resolve a domain failed!!!  please check manually"
            return 2
        fi
    fi
}

check_cpu_mode()
{
    declare -A stable_cpu

    stable_cpu=(['storage-6t']='6 2 Intel(R) Xeon(R) CPU E5-2603 v4 @ 1.70GHz'
                ['storage-8t']='32 2 Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz'
                ['front']='32 2 Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz'
                ['ssd']='32 2 Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz')

    cpu_phy_cores=$(cat /proc/cpuinfo | grep "physical id" \
                   | awk 'BEGIN { max=0 } { if($4>max) max=$4} END{print max+1}')

    cpu_proc_cores=$(cat /proc/cpuinfo | grep "processor" \
                   | awk 'BEGIN{ max=0} {if($3>max) max=$3} END{print max+1}')

    cpu_model=$(cat /proc/cpuinfo | grep "model name" \
              | uniq | awk -F': ' '{print $2}')

    declare -A cpu_info

    cpu_info=(['cpu_model']=$cpu_model \
              ['cpu_phy_cores']=$cpu_phy_cores \
              ['cpu_proc_cores']=$cpu_proc_cores)


    if [[ ${cpu_info[*]} != ${stable_cpu[${type}]} ]];then
        warn "Wrong CPU info: ${cpu_info[*]}, Expect: ${stable_cpu[${type}]}"
    else
        ok "CPU info: ${cpu_info[*]}"
    fi
}

check_disk()
{
    declare -A stable_disk
    stable_disk=(['storage-6t']='SAS 2 279.396GB SATA 12 5.458TB'
                 ['storage-8t']='SAS 2 279.396GB SATA 36 7.277TB'
                 ['front']='SAS 4 558.911GB'
                 ['ssd']='SAS 2 279.396GB SATA 12 1.455TB')

    raid_controller=$(lspci  | grep -i lsi )

    if [[ $(echo ${raid_controller} | grep -e "Fusion-MPT") ]];then

        sys_raid_level=$(sas3ircu 0 DISPLAY | grep -A 3 -B 2 \
                         'Boot                                    : Primary' \
                         | grep 'RAID level                              : RAID1' \
                         | awk -F: '{print $2}' | wc -l)

        sas_disk=$(sas3ircu 0 DISPLAY  | grep -A15 'Device is a Hard disk'\
                   | grep -B8 'Protocol                                : SAS' \
                   | grep Size | uniq -c | sort \
                   | awk '{split($7,a,"/"); size=sprintf("%.3f", a[1]/1024); print "SAS " $1 " " size "GB" }')

        sata_disk=$(sas3ircu 0 DISPLAY  | grep -A15 'Device is a Hard disk'\
                    | grep -B8 'Protocol                                : SATA' \
                    | grep Size | uniq -c | sort \
                    | awk '{split($7,a,"/"); size=sprintf("%.3f", a[1]/1024/1024); print "SATA " $1 " " size "TB"}')

    elif [[ $(echo ${raid_controller} | grep -e 'MegaRAID') ]];then

        sys_raid_level=$(/opt/MegaRAID/MegaCli/MegaCli64 -LdPdInfo -aALL \
                         | grep "RAID Level          : Primary-1" | wc -l)

        sas_disk=$(/opt/MegaRAID/MegaCli/MegaCli64 -PDList -aALL \
                         | grep -A4 "PD Type: SAS" | grep "Raw Size" \
                         | uniq -c | sort | awk '{print "SAS " $1 " " $4 $5}')

        sata_disk=$(/opt/MegaRAID/MegaCli/MegaCli64 -PDList -aALL \
                          | grep -A4 -e 'PD Type: SATA' | grep 'Raw Size' \
                          | uniq -c  | sort | awk '{print "SATA " $1 " " $4 $5}')

    else

      error "unsuport raid controller"

    fi

    if [[ ${sys_raid_level} -eq 1 ]]; then
        ok "system Partition Raid Level: ${sys_raid_level}"
    else
        error "system Partition Raid Level: ${sys_raid_level}, not RAID1!!!"
        return 1
    fi

    disk_info=(${sas_disk} ${sata_disk})

    if [[ ${disk_info[*]} != ${stable_disk[${type}]} ]];then
        warn "Wrong Disk info: ${disk_info[*]}, Expect: ${stable_disk[${type}]}"
    else
        ok "Disk info: ${disk_info[*]}"
    fi
}

check_memory()
{
    declare -A stable_memory
    stable_memory=(['storage-6t']=31
                   ['storage-8t']=62
                   ['front']=62
                   ['ssd']=125)

    memory_size=$(cat /proc/meminfo | grep "MemTotal" | awk '{print $2}')
    memory_size=$[$memory_size/1024/1024]

    if [[ ${memory_size} -ne ${stable_memory[${type}]} ]];then
        warn "Wrong Memory size: ${memory_size}GB, Expect: ${stable_memory[${type}]}GB"
    else
        ok "Memory size: ${memory_size}GB"
    fi
}

setup_root_passwd()
{
    pwd="lB2yg17S.cn"
    echo ${pwd} | passwd --stdin root >/dev/null 2>&1

    if [[ $? == 0 ]];then
        ok "root password has changed"
    else
        error "root password change failed!!!"
    fi
}

setup_ipmi()
{
    ipmi_pwd='Ibsp2m17.cc'
    inner_ip=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}'\
              | grep -e "^192\.168" -e "^172\.[1-3].\.*" -e "^10\.")
    ipmi_ip=$(echo ${inner_ip} | awk '{split($0,arr,".");ipmi_ip=sprintf("%d.%d.%d.%d",arr[1],arr[2],arr[3]+32,arr[4]);print ipmi_ip}')
    ipmi_netmask='255.255.224.0'
    ipmi_gateway='10.102.32.1'
    ipmi_gateway=$(echo ${inner_ip} | awk '{split($0,arr,".");ipmi_gateway=sprintf("%d.%d.%d.%d",arr[1],arr[2],arr[3]+32,1);print ipmi_gateway}')
    ipmitool -I open sensor  >/dev/null 2>&1 || error 'open sensor error'
    ipmitool lan set 1 netmask ${ipmi_netmask} >/dev/null 2>&1 || error 'set netmask error'
    ipmitool lan set 1 defgw ipaddr ${ipmi_gateway} >/dev/null 2>&1 || error 'set defgw error'
    ipmitool lan set 1 ipaddr ${ipmi_ip} >/dev/null 2>&1 || error 'set ipmi ip error'
    ipmitool user set password 2 ${ipmi_pwd} >/dev/null 2>&1 || error 'set ipmi root passwd error'
    ok 'ipmi setup successfully'
}

check_server_hardware()
{
    step "check server <${type}> hardware"
    check_cpu_mode
    check_disk
    check_memory
}

setup()
{
    step "setup system"
    setup_ipmi
    setup_root_passwd
}

check_network
check_os_release
install_package
check_server_hardware
setup
