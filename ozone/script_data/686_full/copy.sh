SRC=/global/cscratch1/sd/tepl/research/spectrum/spectrumsdt/686/level4

# Loop over J dirs
for srcj in $SRC/J*/
do

# Get J dst dir name
srcj="${srcj%/}"     # strip trailing slash
dstj="${srcj##*/}"   # strip path and leading slash
echo $dstj

# Loop over K dirs
for srck in $srcj/K*/
do

# Get K dst dir name
srck="${srck%/}"     # strip trailing slash
dstk="${srck##*/}"   # strip path and leading slash
echo $dstk

# Make dirs
dir1=$dstj/$dstk/3dsdtp
dir2=$dstj/$dstk/chrecog
mkdir -p $dir1
mkdir -p $dir2

# Copy spectrum
cp $srck/3dsdtp/spec.out $dir1/

# Copy channels
srckl=`echo $srck | sed -e "s/level4/level2/g"`
cp $srckl/chrecog/channels.dat $dir2/

# Close loops
done
done

