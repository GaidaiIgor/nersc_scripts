#!/bin/bash -l
#SBATCH -L SCRATCH
#SBATCH -J s4/pes/nonoptimized/mrci-sa/dist/20/angle1/80/angle2/100
#SBATCH -q regular
#SBATCH -N 1
#SBATCH -n 32
#SBATCH -t 1440
#SBATCH -o /global/u2/g/gaidai/marquette/molpro/s4/pes/nonoptimized/mrci-sa/dist/20/angle1/80/angle2/100/input.inp.out
#SBATCH -D /global/u2/g/gaidai/marquette/molpro/s4/pes/nonoptimized/mrci-sa/dist/20/angle1/80/angle2/100
#SBATCH -C haswell



export SLURM_CPU_BIND=cores
export SLURM_CPUS_PER_TASK=4
export JOBDIR=$TMPDIR/$SLURM_JOB_ID
export WFUDIR=$JOBDIR/wfu
mkdir -p $WFUDIR
time molpro -W $WFUDIR --no-xml-output -v -n 16 /global/u2/g/gaidai/marquette/molpro/s4/pes/nonoptimized/mrci-sa/dist/20/angle1/80/angle2/100/input.inp

rm -rf $JOBDIR
