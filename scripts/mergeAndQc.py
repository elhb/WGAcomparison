#! /bin/env python

import sys

# get indata from settings file
indataFile = open(sys.argv[1])
sample = None
indatDist = {}
for line in indataFile:
    if line[0] != "#": raise ValueError
    line = line.rstrip().split(' ')
    if line[1] == 'sample':
        sample = line[2]
        if sample in indatDist: raise KeyError, 'Sample already in indataDist'
        indatDist[sample] = {1:[],2:[]}
    if line[1] == 'fastq':
        indatDist[sample][1].append(line[2])
        indatDist[sample][2].append(line[3])
indataFile.close()

#get the sbatchInfo table
sbatchInfoFile = open('sbatchInfoDist.txt')
sbatchInfo = eval(sbatchInfoFile.read())
sbatchInfoFile.close()

print sbatchInfo
for sample in sbatchInfo:
    sbatchInfo[sample]['merging'] = {'file':None,'jobid':None}
    tmpCounter0Values = sbatchInfo[sample]['mapping']['jobids'].keys()
    sbatchInfo[sample]['merging']['file'] = open('tasks/mapping/'+sample+'/sbatch/mergeAndQC.sh','w')
    sbatchInfo[sample]['merging']['file'].write(
        '#! /bin/bash -l\n'+
        '#SBATCH -A b2011011\n'+
        '#SBATCH -n 8 -p node\n'+
        '#SBATCH -t 2:00:00\n'+
        '#SBATCH -J merging.'+sample+'\n'+
        '#SBATCH -e /proj/b2011011/WGAcomparison/tasks/mapping/'+sample+'/sbatch/stderr.mergeAndQC.txt\n'+
        '#SBATCH -o /proj/b2011011/WGAcomparison/tasks/mapping/'+sample+'/sbatch/stdout.mergeAndQC.txt\n'+
        '#SBATCH --mail-type=All\n'+
        '#SBATCH --mail-user=erik.borgstrom@scilifelab.se\n'+
        'cd /proj/b2011011/WGAcomparison/\n'+
        'picard=/bubo/proj/b2011168/private/bin/picard-tools-1.63\n'
        'java -Xmx2g -jar $picard/MergeSamFiles.jar MAX_RECORDS_IN_RAM=2500000 '+' '.join(['INPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.rgInfoFixed.bam' for tmpCounter0 in tmpCounter0Values])+' OUTPUT=tasks/mapping/'+sample+'/'+sample+'.merged.bam CREATE_INDEX=true USE_THREADING=true\n'
        'java -Xmx2g -jar $picard/CalculateHsMetrics.jar BAIT_INTERVALS=references/AgilentSureSelectHumanAllExonV4/S03723314_Covered.bed TARGET_INTERVALS=references/AgilentSureSelectHumanAllExonV4/S03723314_Covered.bed INPUT=tasks/mapping/'+sample+'/'+sample+'.merged.bam OUTPUT=tasks/mapping/'+sample+'/'+sample+'.hs_metrics.summary.txt PER_TARGET_COVERAGE=tasks/mapping/'+sample+'/'+sample+'.perTargetCov.txt REFERENCE_SEQUENCE=references/hg19/concat.fa\n'+
        'java -Xmx2g -jar $picard/CalculateHsMetrics.jar BAIT_INTERVALS=references/AgilentSureSelectHumanAllExonV4/S03723314_Covered.bed TARGET_INTERVALS=references/AgilentSureSelectHumanAllExonV4/S03723314_Covered.bed INPUT=tasks/mapping/'+sample+'/'+sample+'.merged.bam OUTPUT=tasks/mapping/'+sample+'/'+sample+'.hs_metrics.summary.txt PER_TARGET_COVERAGE=tasks/mapping/'+sample+'/'+sample+'.perTargetCov.txt REFERENCE_SEQUENCE=references/hg19/concat.fa\n'+
        'samtools flagstat tasks/mapping/'+sample+'/'+sample+'.merged.bam\n'
    )
    sbatchInfo[sample]['merging']['file'].close()

# submitt the jobs
import subprocess
for sample in sbatchInfo:
    jobids     = sbatchInfo[sample]['mapping']['jobids'].values()
    sbatchFile = sbatchInfo[sample]['merging']['file']
    sbatchInfo[sample]['merging']['file'] = sbatchFile.name
    args = ['sbatch',sbatchFile.name, '--dependency=afterok:'+':'.join([str(jobid) for jobid in jobids]) ]
    print args
    sbatch = subprocess.Popen( args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    sbatch_out, sbatch_err = sbatch.communicate()
    if sbatch.returncode != 0:
        print 'sbatch view Error code', sbatch.returncode, sbatch_err
        print sbatch_out
        sys.exit()
    sbatchInfo[sample]['merging']['jobid'][tmpCounter0] = sbatch_out.split('\n')[0].split(' ')[3]

# save the info to disk
sbatchInfoFile = open('sbatchInfoDist.txt','w')
sbatchInfoFile.write(str(sbatchInfo))
sbatchInfoFile.close()
