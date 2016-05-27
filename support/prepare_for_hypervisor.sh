## Install a necessary packages
sudo apt install cloud-utils genisoimage

## Go to /var/lib/libvirt/images
present_dir=$PWD
cd /var/lib/libvirt/images

## URL to most recent cloud image of 12.04
img_url="https://cloud-images.ubuntu.com/xenial/current/"
img_url="${img_url}/xenial-server-cloudimg-amd64-disk1.img"
## download the image
sudo wget $img_url -O disk.img.dist

## Convert the compressed qcow file downloaded to a uncompressed qcow2
sudo qemu-img convert -O qcow2 disk.img.dist disk.img.orig

## Create a delta disk to keep our .orig file pristine
sudo qemu-img create -f qcow2 -b disk.img.orig disk.img

cd $present_dir
