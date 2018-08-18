# embed sourced file into each


fns  = init-centos-7.sh
# fns += $(shell cd src; ls centos*-install)
# fns += mac-install


all:
	for f in $(fns); do cat $$f | awk -f preproc.awk > ../../bin/$$f && chmod +x ../../bin/$$f; done
	cp create-vagrant-vm-mac.sh ../../bin/
