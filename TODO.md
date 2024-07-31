* Add a logger: Print all the infos into various log files.
* Export a register dataset to .reg file: Create a file that contains all the informations to replicate the download from one computer to another.
* Create a register from a .reg file: Load a .reg file and locally creates the register from it.
* Cleaning on download failure: If a dataset download has failed, the tmp directories has to be cleaned to allow a retry.
* Saving already downloaded files: Create a downloaded files register to avoid downloading multiple time the same file.