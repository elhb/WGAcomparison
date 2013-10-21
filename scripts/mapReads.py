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

#create the sbatch files
sbatchInfo = {}
import os
for sample in indatDist:
    tmpCounter0 = 0
    sbatchInfo[sample] = {'mapping':{'files':{},'jobids':{}}}
    try: os.makedirs('tasks/mapping/'+sample+'/sbatch/')
    except OSError: pass
    for r1File,r2File in zip(indatDist[sample][1],indatDist[sample][2]):
        tmpCounter0+=1
        sbatchInfo[sample]['mapping']['files'][tmpCounter0] = open('tasks/mapping/'+sample+'/sbatch/'+str(tmpCounter0)+'.sh','w')
        sbatchInfo[sample]['mapping']['files'][tmpCounter0].write(
            '#! /bin/bash -l\n'+
            '#SBATCH -A b2011011\n'+
            '#SBATCH -n 8 -p node\n'+
            '#SBATCH -t 2:00:00\n'+
            '#SBATCH -J mapping.'+sample+'.'+str(tmpCounter0)+'\n'+
            '#SBATCH -e tasks/mapping/'+sample+'/sbatch/stderr.'+str(tmpCounter0)+'.txt\n'+
            '#SBATCH -o tasks/mapping/'+sample+'/sbatch/stdout.'+str(tmpCounter0)+'.txt\n'+
            '#SBATCH --mail-type=All\n'+
            '#SBATCH --mail-user=erik.borgstrom@scilifelab.se\n'+
            'bowtie2 -x references/hg19/concat -1 '+r1File+' -2 '+r2File+' -S tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.sam -p 8\n'+
            'picard=/bubo/proj/b2011168/private/bin/picard-tools-1.63\n'+
            'java -Xmx2g -jar $picard/SamFormatConverter.jar MAX_RECORDS_IN_RAM=2500000 INPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.sam OUTPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.bam\n'+
            'java -Xmx2g -jar $picard/SortSam.jar MAX_RECORDS_IN_RAM=2500000 SORT_ORDER=coordinate INPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.bam OUTPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.sorted.bam CREATE_INDEX=true\n'+
            'java -Xmx2g -jar $picard/MarkDuplicates.jar MAX_RECORDS_IN_RAM=2500000 VALIDATION_STRINGENCY=LENIENT INPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.sorted.bam OUTPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.marked.bam METRICS_FILE=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.MarkDupsMetrix CREATE_INDEX=true\n'+
            'java -Xmx2g -jar $picard/AddOrReplaceReadGroups.jar MAX_RECORDS_IN_RAM=2500000 INPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.marked.bam OUTPUT=tasks/mapping/'+sample+'/'+str(tmpCounter0)+'.rgInfoFixed.bam CREATE_INDEX=true RGID='+sample+' RGLB='+sample+' RGPL=illumina RGSM='+sample+' RGCN="SciLifeLab" RGPU="barcode" \n'
        )
        sbatchInfo[sample]['mapping']['files'][tmpCounter0].close()

# submitt the jobs
import subprocess
for sample in sbatchInfo:
    for tmpCounter0, sbatchFile in sbatchInfo[sample]['mapping']['files'].iteritems():
        args = ['sbatch',sbatchFile.name]
        sbatch = subprocess.Popen( args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        sbatch_out, sbatch_err = sbatch.communicate()
        if sbatch.returncode != 0:
            print 'sbatch view Error code', sbatch.returncode, sbatch_err
            print sbatch_out
            sys.exit()
        sbatchInfo[sample]['mapping']['jobids'][tmpCounter0] = sbatch_out.split('\n')[0].split(' ')[3]

# save the info to disk
sbatchInfoFile = open('sbatchInfoDist.txt','w')
sbatchInfoFile.write(str(sbatchInfo))
sbatchInfoFile.close()

#JUNK:      'java -Xmx2g -jar $picard/CalculateHsMetrics.jar BAIT_INTERVALS=?.bed TARGET_INTERVALS=?.bed INPUT= OUTPUT=.hs_metrics.summary.txt PER_TARGET_COVERAGE=..txt REFERENCE_SEQUENCE='

