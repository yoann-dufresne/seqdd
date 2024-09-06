* Saving already downloaded files: Create a downloaded files register to avoid downloading multiple time the same file.
* Extend NCBI downloads to other data type than genomes
* Extend ENA downloads to other data types than raw reads
* NCBI: Use the output of datasets on wrong accession number to remove the bad accessions only (See how it is recursively done in SRA)
* Change the delay functions to hold an amount of query per second, not a fix delay
* Add url support for s3
* Add url support for gs
* Allow download path hierarchy