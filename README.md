#WGAcomparison

##Usage
###Indatafile
First you need to create a indata file with the following format:  

    # sample SAMPLEID_1
    # fastq R1_FILE R2_FILE
    # sample SAMPLEID_2
    # fastq R1_FILE R2_FILE
    ...
    # sample SAMPLEID_n
    # fastq R1_FILE R2_FILE

###Mapping
To map the data run:  
  
    python scripts/mapReads.py indata.txt
### Merging mapped bamfiles and do some QC
To merge the bamfiles and calculate HS metrics run:  
  
    python scripts/mergeAndQc.py indata.txt
### Call variants
Not implemented.
###Create summary
Not implemented.
