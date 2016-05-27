#Apache Mesos

Apache Mesos is a cluster manager that provides efficient resource isolation and sharing across distributed applications, or frameworks. It can run Hadoop, MPI, Hypertable, Spark, and other frameworks on a dynamically shared pool of nodes.

#Qemu/KVM Containerizer

This fork is a very basic implementation to support Qemu/KVM containerization.
A detailed design doc can be found in the following [link](https://docs.google.com/document/d/1_VuFiJqxjlH_CA1BCMknl3sadlTZ69FuDe7qasDIOk0/edit?ts=56fab906).

The related MESOS Jira can be found [here](https://issues.apache.org/jira/browse/MESOS-2717)

#Authors
 * Abhishek Dasgupta - <a10gupta@linux.vnet.ibm.com>
 * Pradipta Kumar Banerjee - <bpradip@in.ibm.com>

#Installation

* ##Pre-requisites
Following packages are required on the host: qemu, libvirt and libvirt devel
Use distribution package repositories to install libvirt and qemu.
On Red Hat based systems the required packages can be installed using the following command:
<pre>
    yum install libvirt-daemon libvirt-daemon-driver-kvm libvirt-daemon-driver-qemu libvirt-devel qemu qemu-kvm
</pre>
On Ubuntu based systems the required packages can be installed using the following command:
<pre>
    apt install libvirt-bin qemu libvirt-dev
</pre>

* ##Download and Prepare the Guest Image
<pre>
 ./support/prepare_for_hypervisor.sh
</pre>
This script pulls cloud image for ubuntu xenial distro, stores it to /var/lib/libvirt/images directory.
The cloud images comes pre-installed with cloud-init which is mandatory requirement for the guest image.
Modify the script as per your requirement to pull any other distro cloud image of your choice.

* ##Note
While we have tested this using Linux based guests only, the same should work for Windows based guests having cloud-init or equivalent. Here are some good references:
    1. [Cloud-Init for Windows](https://cloudbase.it/cloudbase-init/)
    2. [Adding Cloud-init to to Windows](https://www.ibm.com/support/knowledgecenter/SS4KMC_2.5.0/com.ibm.ico.doc_2.5/c_adding_cloud_init_to_windows_i.html)


#Build the Code
Now as just everything in place, it is time for building the code:

<pre>
 sudo ./bootstrap
 mkdir build && cd build
 sudo ../configure
 sudo make -j4
 sudo GTEST_FILTER="" make -j4 check
</pre>

#Testing the code
Go to the build directory and
Run the master locally
<pre>
 sudo ./bin/mesos-master.sh --ip=127.0.0.1 --work_dir=/var/lib/mesos
</pre>
Run the slave/agent locally
<pre>
 sudo ./bin/mesos-slave.sh --master=127.0.0.1:5050 --ip=0.0.0.0 --containerizers=hypervisor --work_dir=/home/user/mesos_workdir
</pre>
Run a toy framework
<pre>
 sudo ./no-executor-framework --master=master@127.0.0.1:5050 --command="touch /bombino.txt" --num_tasks=1
</pre>
Locate the newly running VM with name as last 12 digit of the container ID (Check the slave log for the VM ID)
Using virt-manager/virsh or any similar tool of your choice login to the VM with username as "ubuntu" and password as "passw0rd". You will see "/bombino.txt" has been created. If you want to run some other command, start the example framework with some other command as a value of "--command" option.

#Note
This fork is heavily under development and may produce lots of bugs while running in a little fancier way than mentioned.
Please be patient and request your help fixing it.
