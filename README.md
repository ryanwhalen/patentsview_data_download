# patentsview_data_download
A simple Python script to download public patentsview data and make relational database

Patentsview.org shares high-quality USPTO data. This script will download the various files shared by patentsview on their data download page and use them to create a sqlite database.

By default the script will download the publicly-available files. If you wish to also download the detailed description files, you will need to manually at the URLs for those to the "detailed_descs" list. Details on how to obtain those URLs can be found on patentsivew.org/downloads.

Dependencies: BeautifulSoup, Pandas, and curl.

Downloading may take a few days depending on your internet speed. This has been tested on a machine with 128GB of RAM. If you're working with less RAM, you might need to tweak the parse_tsv() function to chunk the input file.

The database tracks files that have been successfully downloaded and added, so if you run into a problem mid-download (e.g. your Internet goes down, or your power goes out), you should be able to run the script again and it will re-start on the last file you were attempting to download.

The database indexing done in the script is very general. You may wish to tweak the indexing for your specific use case.

To run, simply put the script in the directory where you want the database to be created. Ensure you have the required dependencies installed and then from your terminal—or the interactive Python environment of your choice—run the file. E.g:

>python patentsview_download.py
