## Create a vagrant VM on Mac

-   Make a directory for VM(vagrant needs a dir for every VM), e.g.:

    ```
    mkdir ~/va-centos-7
    cd ~/va-centos-7
    ```

-   Create and start centos-7 VM:

    ```
    curl https://raw.githubusercontent.com/bsc-s2/ops/master/bin/create-vagrant-vm-mac.sh | sh
    ```

-   Enter VM via ssh:

    ```
    vagrant ssh
    ```

## Init a vagrant VM for dev:

```
curl https://raw.githubusercontent.com/bsc-s2/ops/master/bin/init-centos-7.sh | sh
```
