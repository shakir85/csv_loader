# CSV Files Loader

## Project Description

Python program to unzip locally hosted archive and then loads a specific CSV file to MySQL table. This program is intended to demonistrate the concept of data extracting and loading tasks. The program is strcutured to handle data set of historical real estate prices of Sacramento county, California. However, it can be altered to manage similar data extraction jobs.

## Program Approach

_How to tackle the problem and why?_

The approach is to dump the unzipped content to a temporary folder on disk, do the database insert task, and then get rid of the temp folder to eliminate redundant data on disk.

Writing unzipped content to a disk rather than holding them in memory via using `StringIO` and `BytesIO` streams seems less efficient, but that's not true from memory management point of view.

Holding intermediate data on memory will fail for the following reasons:

- Server instances will run out of memory with huge data sets.
- Risk of cost-increase when working on cloud instance (i.e. AWS EC2) while memory auto-scaling is enabled.
- Disks are cheaper than memory in terms of cost & failures, as long as proper configurations of distributed storage and distributed filesystem on place.
- Unzipping files on disk allows us to chain multiple files unzipping-tasks in a single program run.

However, there are some trade-offs of using disks, such as:

- Network congestion.
- Slower disk IO (especially with non-SSD disks).
- Performance dependability on storage and filesystem architectures.
- If reading/writing to/from cloud storage, additional factors should be considered such as cost, storage configurations, retrieving data from archiving systems ...etc

### Program Structure

_Per the code sequence:_

- **First function (Files Processing):**

  - Walk through the file system to select specific zip archive.
  - Creates a temporary directory in disk to hold unzipped content.
  - Selects the desired CSV file to be loaded into a MySQL table.

- **Second function (Database Jobs):**
  - Connect to a local MySQL DB server.
  - Create specific table based on the CSV file schema.
  - Iterate through the CSV file and insert the data into MySQL table.
  - Delete temporary directory.
