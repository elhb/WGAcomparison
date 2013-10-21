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
### Merging mapped bamfiles
Not implemented.
### Do some QC
Not implemented.
### Call variants
Not implemented.
###Create summary
Not implemented.
