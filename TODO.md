* Create a register from a .reg file: Load a .reg file and locally creates the register from it.
* Verify data availability on add
* Verify data availability before download
* Add a logger: Print all the infos into various log files.
* Cleaning on download failure: If a dataset download has failed, the tmp directories has to be cleaned to allow a retry.
* Saving already downloaded files: Create a downloaded files register to avoid downloading multiple time the same file.
* Add url support for s3
* Add url support for gs
* Add a name per file with url
* Allow download path hierarchy