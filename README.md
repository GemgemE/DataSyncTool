# Data synchronisation tool

## Locations of key functions:

`upload_files_since_last_sync_with_retry()` - Line 124\
    Contains the retry logic if the connection fails. The package Tenacity is
    being used for the retry logic, it allows for a retry condition and for the
    number of retries to be defined.
    This function is also where the connection to the remote server is
    established using the package Paramiko. Paramiko allows for cross-platform
    connections provided both sides have sftp capabilities.
    Each time a file is created or modified between syncs it is added to the set
    files_to_sync, in this upload function that set is looped through and each
    file is uploaded to the remote server, should it fail the set is not cleared
    and the upload can be attempted again.
    After the upload is considered successful the md5 hash of both the local
    and remote file are calculated. If they match then the upload is considered
    finished and successful. If they don't match then the upload is triggered
    again until they do.

`on_created()` -  Line 218\
    This function like on_modified() is inherited from the
    FileSystemEventHandler class and overwritten to provide custom logic when an
    event is triggered. Some os (windows, unsure of linux) trigger multiple
    on_created and on_modified events when files are created or modified. To
    avoid uploading a file for duplicate events the files that these events are
    triggered for are recorded in files_to_sync which is a set so there can't be
    duplicate uploads.

`calculate_remote_md5_hash()` - Line 329\
    This function is where the md5 hash is calculated for the entire file, the
    default chunk size is 8KB in an attempt to reduce the amount of data read
    at a time. Doing the hash of the entire file can be time consuming but
    allows for more certainty that the file sent matches the local file.

`upload_with_resume()` - Line 363\
    In order to allow for the resuming of uploads this function starts
    uploading to a .part file. Before the upload is commenced it checks if a
    .part file already exists, if it does it uses seek() to find the end of the
    file and then writes to it in chunks, again the default chunk size is 8KB in
    an attempt to reduce the amount of data sent at a time. When the file is
    uploaded completely the .part file is renamed to remove the .part.
