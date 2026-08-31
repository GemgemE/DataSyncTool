"""Data synchronisation tool

Locations of key functions:

upload_files_since_last_sync_with_retry() - Line 124
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

on_created() -  Line 218
    This function like on_modified() is inherited from the
    FileSystemEventHandler class and overwritten to provide custom logic when an
    event is triggered. Some os (windows, unsure of linux) trigger multiple
    on_created and on_modified events when files are created or modified. To
    avoid uploading a file for duplicate events the files that these events are
    triggered for are recorded in files_to_sync which is a set so there can't be
    duplicate uploads.

calculate_remote_md5_hash() - Line 329
    This function is where the md5 hash is calculated for the entire file, the
    default chunk size is 8KB in an attempt to reduce the amount of data read
    at a time. Doing the hash of the entire file can be time consuming but
    allows for more certainty that the file sent matches the local file.

upload_with_resume() - Line 363
    In order to allow for the reusuming of uploads this function starts
    uploading to a .part file. Before the upload is commenced it checks if a
    .part file already exists, if it does it uses seek() to find the end of the
    file and then writes to it in chunks, again the default chunk size is 8KB in
    an attempt to reduce the amount of data sent at a time. When the file is
    uploaded completely the .part file is renamed to remove the .part.

"""

import paramiko
import os
import hashlib
import argparse
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import time
from watchdog.events import (
    FileSystemEventHandler,
    DirCreatedEvent,
    FileCreatedEvent,
    DirModifiedEvent,
    FileModifiedEvent,
)
from watchdog.observers import Observer
from threading import Timer


class Handler(FileSystemEventHandler):
    """A custom event handler that defines actions for creation and modification
     events.

    Inherits from the FileSystemEventHandler watchdog class.
    """

    def __init__(
        self,
        remote_dest_dir: str,
        hostname: str,
        port: int = 22,
        username: str | None = None,
        key_path: str | None = None,
        password: str | None = None,
        chunk_size: int = 8192,
    ) -> None:
        """Initialise the Handler class.

        Also starts the sync timer. password or key_path can be used to ssh to
        the remote server.

        Args:
            remote_dest_dir: a str of the destination directory on the remote
                server
            hostname: a str of the host name needed to connect to the remote
                server
            port: int of the port to connect to, default is 22
            username: a str of the username needed to connect to the remote
                server
            key_path: a str of the path to the private key file
            password: a str of the password related to the username used to
                connect to the remote server
        """
        self.remote_dest_dir = remote_dest_dir
        self.hostname = hostname
        self.port = port
        self.username = username
        self.key_path = os.path.expanduser(key_path)
        self.chunk_size = chunk_size
        self.password = password
        self.ssh = None
        self.sftp = None
        self.files_to_sync = set()

        # start the sync timer, every 1 minute any files created or modified
        # since the last sync will be uploaded to the remote server
        self.schedule_next(1)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(
            (
                paramiko.SSHException,
                OSError,
                IOError,
            )
        ),
        reraise=True,
    )
    def upload_files_since_last_sync_with_retry(self) -> None:
        """Loop through all files in files_to_sync and upload them.

        This function calls schedule_next at the end to continuously sync files.
        Any file that has been created or modified since the last call will be
        in files_to_sync. upload_with_retry is called for each file in
        files_to_sync.

        Once the file is uploaded the md5 hash of the local and remote files
        is calculated and compared. If they do not match then the upload is
        triggered again until they do.
        """

        print(
            "Syncing all files that have been modified or created since "
            "last sync...\n"
        )
        if not self.files_to_sync:
            print("No files have been created or modified since last sync.\n")
        try:
            self.connect()
            for file in self.files_to_sync:
                file_name = os.path.basename(file)
                remote_file_path = self.remote_dest_dir + file_name
                files_match = False

                # upload the select file to the remote server
                # if the md5 hash for the local file and for the remote file
                # don't match then redo the upload
                while not files_match:
                    print(
                        f"{file} has been created or modified since last sync, "
                        f"commencing new upload...\n"
                    )
                    self.upload_with_resume(
                        file,
                        remote_file_path,
                    )
                    # Check if complete
                    part_path = remote_file_path + ".part"
                    try:
                        self.sftp.stat(part_path)
                        # part file is found, not complete, will retry
                        raise IOError("Transfer incomplete... attempting again.")
                    except FileNotFoundError:
                        # part file is not found, transfer is complete
                        print("Transfer Complete.\n")

                    watched_file_size = os.path.getsize(file)
                    remote_size = self.get_remote_size(remote_file_path)

                    # if the file is empty then don't bother calculating the
                    # md5 hashes
                    if watched_file_size != 0 and remote_size != 0:
                        print("Checking files match...")

                        watched_md5_hash = self.calculate_local_md5_hash(file)
                        print(f"Local MD5: {watched_md5_hash}")

                        remote_md5_hash = self.calculate_remote_md5_hash(
                            remote_file_path
                        )
                        print(f"Remote MD5: {remote_md5_hash}")

                        if watched_md5_hash != remote_md5_hash:
                            print("Files are not the same, retrying upload...")
                        else:
                            print("Files match. Transfer Finished.\n")
                            files_match = True
                    else:
                        print("Files match. Transfer Finished.\n")
                        files_match = True

        finally:
            # reset the set so that the files from the previous sync are not
            # uploaded again
            # if an IOError is raised then this is not reset and an upload can
            # be attempted again using the .part files
            self.files_to_sync = set()
            # Reschedule the next execution (currently every 1 minute)
            self.schedule_next(1)
            self.disconnect()

    def schedule_next(self, minutes: int) -> None:
        """Schedule the next time upload_files_since_last_sync_with_retry calls.

        Currently set to every 1 minute.

        Args:
            minutes: int for the number of minutes between each function call
        """
        t = Timer(minutes * 60, self.upload_files_since_last_sync_with_retry)
        t.start()

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        """Whenever a file is created add it to files_to_sync.

        Inherited from the FileSystemEventHandler class, on_created is called
        everytime a file is created in the directory being watched.

        Args:
            event: the event call that is created when something happens in
                the directory being watched
        """
        # ignore directory events, only files need to be copied
        if event.is_directory:
            return

        watched_file_path = event.src_path

        # on_created is also triggered when a file is modified, modified files
        # have a ~ on the end of the filename, skip these files
        if watched_file_path.endswith("~"):
            return

        self.files_to_sync.add(watched_file_path)

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        """Whenever a file is modified add it to files_to_sync.

        Inherited from the FileSystemEventHandler class, on_modified is called
        everytime a file is modified in the directory being watched.

        Args:
            event: the event call that is created when something happens in
                the directory being watched
        """
        # ignore directory events, only files need to be copied
        if event.is_directory:
            return

        watched_file_path = event.src_path

        # modified files end with ~, this needs to be removed so that the file
        # can be found
        if watched_file_path.endswith("~"):
            watched_file_path = watched_file_path.replace("~", "")

        self.files_to_sync.add(watched_file_path)

    def connect(self):
        """Establish SSH and SFTP connections using paramiko.

        Auto detects the kind of key being used if a key is used instead of a
        password.
        """
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.RejectPolicy())

        if self.key_path:
            # Auto-detect key type
            key_classes = [
                paramiko.Ed25519Key,
                paramiko.RSAKey,
                paramiko.ECDSAKey,
            ]
            private_key = None
            for key_class in key_classes:
                try:
                    private_key = key_class.from_private_key_file(self.key_path)
                    break
                except paramiko.SSHException:
                    continue
            if private_key is None:
                raise paramiko.SSHException(f"Unable to load key from {self.key_path}")
            self.ssh.connect(
                self.hostname,
                port=self.port,
                username=self.username,
                pkey=private_key,
            )
        else:
            self.ssh.connect(
                self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
            )

        self.sftp = self.ssh.open_sftp()

    def disconnect(self):
        """Close SFTP and SSH connections."""
        if self.sftp:
            self.sftp.close()
            self.sftp = None
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def get_remote_size(self, remote_file_path: str) -> int:
        """Return size of remote file.

        If the file does not exist on the remote server then a 0 is returned.

        Args:
            remote_file_path: string of the file path on the remote server
        """
        try:
            file_size = self.sftp.stat(remote_file_path).st_size
        except Exception:
            file_size = 0
        return file_size

    def calculate_remote_md5_hash(self, remote_file_path: str) -> str:
        """Calculate MD5 hash of entire remote file.

        Args:
            remote_file_path: str of the file path on the remote server
        """
        md5 = hashlib.md5()

        with self.sftp.open(remote_file_path, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                md5.update(chunk)

        return md5.hexdigest()

    def calculate_local_md5_hash(self, watched_file_path: str) -> str:
        """Calculate MD5 hash of entire local file.

        Args:
            watched_file_path: str of the file path on the local computer
        """
        md5 = hashlib.md5()

        with open(watched_file_path, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                md5.update(chunk)

        return md5.hexdigest()

    def upload_with_resume(
        self,
        watched_file_path: str,
        remote_file_path: str,
    ) -> int:
        """Upload a file with resume support.

        Write the local file to the remote file in chunks, if there is a
        partial file it will end with .part, using the seek the upload can be
        resumed from where the partial file ends.

        Args:
            watched_file_path: str of the local file to be watched
            remote_file_path: str of the remote file where files will be
                uploaded
        """
        part_path = remote_file_path + ".part"

        watched_file_size = os.path.getsize(watched_file_path)
        remote_size = self.get_remote_size(part_path)

        # Handle empty files, they will be created but nothing needs to be
        # written to them
        if watched_file_size == 0:
            print("Local file is empty (0 bytes)")
            open(remote_file_path, "w").close()
            try:
                # check if the .path file exists in the remote server, remove it
                # if so
                self.sftp.stat(part_path)
                self.sftp.remove(part_path)
                return 0
            except FileNotFoundError:
                return 0

        # if the file already exists then delete it and rename the .part
        # file to the name of the deleted file, this allows old files to be
        # written over with the new changes
        if remote_size >= watched_file_size:
            print(f"File already complete ({remote_size:,} bytes)")
            try:
                # check if the .path file exists in the remote server, remove it
                # if so and rename the .path file
                self.sftp.stat(part_path)
                self.sftp.remove(remote_file_path)
                self.sftp.rename(part_path, remote_file_path)
                return 0
            except FileNotFoundError:
                return 0

        print(f"Local: {watched_file_size:,} bytes")
        print(f"Remote:  {remote_size:,} bytes")
        if remote_size > 0:
            print(f"Resuming from byte {remote_size:,}")

        bytes_uploaded = 0

        # write the local file to the remote file in chunks, if there is a
        # partial file it will end with .part, using the seek the upload can be
        # resumed from where the partial file ends
        with open(watched_file_path, "rb") as local_file:
            local_file.seek(remote_size)

            with self.sftp.open(part_path, "ab") as remote_file:
                while True:
                    chunk = local_file.read(self.chunk_size)
                    if not chunk:
                        break
                    remote_file.write(chunk)
                    bytes_uploaded += len(chunk)

                    total = remote_size + bytes_uploaded
                    percent = (total / watched_file_size) * 100
                    print(
                        f"\rProgress: {percent:.1f}% ({total:,}/"
                        f"{watched_file_size:,} bytes)",
                        end="",
                        flush=True,
                    )

        final_size = self.get_remote_size(part_path)
        if final_size == watched_file_size:
            try:
                # check if the remote_file already exists in the remote server,
                # remove it if so and rename the .path file to takes its place
                self.sftp.stat(remote_file_path)
                self.sftp.remove(remote_file_path)
                self.sftp.rename(part_path, remote_file_path)
                print(f"Complete: {remote_file_path}")
            except FileNotFoundError:
                self.sftp.rename(part_path, remote_file_path)
                print(f"Complete: {remote_file_path}")
        else:
            print(
                f"Size mismatch: expected {watched_file_size:,}, " f"got {final_size:,}"
            )
        return bytes_uploaded


def main():
    parser = argparse.ArgumentParser(
        description="Upload files to a remote server..",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage="%(prog)s  <dir_to_watch> <dir_to_send> [options]",
    )
    parser.add_argument(
        "dir_to_watch",
        help="Path to directory that will be watched",
    )
    parser.add_argument("dir_to_send", help="Destination path")
    parser.add_argument(
        "--host",
        help="SFTP server hostname.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=22,
        help="SFTP server port (default: 22)",
    )
    parser.add_argument(
        "--user",
        help="SFTP username.",
    )
    parser.add_argument(
        "--key",
        help="Path to SSH private key.",
    )
    parser.add_argument(
        "--password",
        help="SFTP password. Use --key instead when possible.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=8192,
        help="Upload chunk size in bytes (default: 8192)",
    )

    args = parser.parse_args()

    # Validate required arguments
    if not args.host:
        parser.error("--host is required")
    if not args.user:
        parser.error("--user is required")
    if not args.key and not args.password:
        parser.error("Either --key or --password is required")

    print(f"Connecting to {args.user}@{args.host}:{args.port}")
    print(f"dir to watch: {args.dir_to_watch}")
    print(f"dir to send:  {args.dir_to_send}")

    # create the event handler that will handle create and modify events
    event_handler = Handler(
        args.dir_to_send,
        hostname=args.host,
        port=args.port,
        username=args.user,
        key_path=args.key if args.key else None,
        password=args.password if args.password else None,
        chunk_size=args.chunk_size,
    )
    # Observer object is what watches the directories
    observer = Observer()
    observer.schedule(event_handler, path=args.dir_to_watch, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main()
