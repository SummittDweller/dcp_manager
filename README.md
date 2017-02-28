# dcp_manager
New Python-based DCP Manager for the Wieting Theatre, Toledo, IA.

##Python Dependencies
   - xmltodict
   - lxml

Use **sudo apt install python-pip** to install **pip** if necessary.
Use **```pip install xmltodict```** and **```pip install lxml```** as necessary to install these Python dependencies.
 
##Other Dependencies
This script also relies on **~/.netrc** to store and serve up protected credentials for SMTP email dispatch.

##Typical Use
    python dcp_manager.py COPY --source=`/media/booth/tdc_4387 --packages=13Minut*
    python dcp_manager.py DELETE --packages=13Minut*
    python dcp_manager.py CATALOG 
    
###Operations
**COPY** - Copies pacakge assets from --source to --destination.

**DELETE** - Removes named --packages assets from --destination.
 
**CATALOG** - Catalogs the package assets in --source to a new ASSETMAP file.
 
###Options
| Option | Description | Default |
|:------:|-------------|---------|
|--source=directory | The source directory for all operations. | /mnt/trailers |
|--destination=directory | The destination directory for the COPY operation. | /mnt/trailers |
|--mail=address | Email address to receive operation logs. | toledowieting@gmail.com |
|--logfile=path | Logfile to capture operation output. | /tmp/dcp_manager.log |
|--packages=pattern | Package name pattern to match for COPY and DELETE. | None |
|--debug | Output debug info. | False |
|--scope | Include SCOPE format packages in COPY and CATALOG operations. | False |
|--not_tlr | Include non-TLR named packages in COPY and CATALOG operations. | False |


 



