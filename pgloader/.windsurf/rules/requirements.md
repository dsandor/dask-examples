---
trigger: always_on
---

* allow a command line argument that takes a 'import-root' directory.
* argument to allow specifying a regex to match directory names to include.
* argument to allow specifying a regex to match directory names to exclued.
* if an import-root is specified enumerate each directory that matches the include but does not match an exclue.
* locate the most recent csv.gz file by examinging the filename which includes a date in the format YYYYMMDD
* unzip the csv into the ./import folder.
* run the import code using that file.