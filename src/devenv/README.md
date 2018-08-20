#  Create a vagrant VM on Mac

Create a vagrant VM on Mac as a personal development enviroment in 2 steps.

#   Usage

##  Create and start centos-7 VM:

Make a directory for VM(vagrant needs a dir for every VM), e.g.:

```
mkdir ~/va-centos-7
cd ~/va-centos-7
```

Run following script install `vagrant` via `brew`, download centos box if not found,
then create a VM.

```
curl https://raw.githubusercontent.com/bsc-s2/ops/master/bin/create-vagrant-vm-mac.sh | sh
```

## Init a vagrant VM for dev:

Enter VM via ssh:

```
vagrant ssh
```

Run following script inside VM to install packages etc.

```
curl https://raw.githubusercontent.com/bsc-s2/ops/master/bin/init-centos-7.sh | sh
```

**Of course you can use this script to init other non-vagrant VM for
development**.
Using it to init a production server is OK too.


#   Contribute

A Makefile builds the above two script: `bin/create-vagrant-vm-mac.sh` and
`bin/init-centos-7.sh`.
Do not edit these files directly.
Instead, modify files in this folder and `make` to re-generate.

-   Packages(yum and pip) to install are defined in `init-centos-7.sh`.

-   Resource file such as `.vimrc` are defined in standalone `vimrc`(without the prefix dot)

    We have a small script `preproc.awk` that replace a place holder such as `source
    vimrc` with the content of file `vimrc`.
