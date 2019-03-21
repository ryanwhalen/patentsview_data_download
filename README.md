# patentsview_data_download
Tool to download public patentsview data and make relational database

Patentsview.org shares high-quality USPTO data. This script will download the various flat files shared by patentsview and use them to create a sqlite database.

By default the script will download the publicly-available files. If you wish to also download the detailed description files, you will need to manually at the URLs for those to the "detailed_descs" list. Details on how to obtain those URLs can be found on patentsivew.org/downloads.

Dependencies: BeautifulSoup, Pandas, and curl.

Downloading will probably take a few days depending on your internet speed. R\This has been tested on a machine w/ 128GB of RAM and it works. If you're working with less RAM, you might need to tweak the parse_tsv() function to chunk the input file.
