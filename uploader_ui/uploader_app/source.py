import os
import logging
import re
from abc import ABCMeta
from typing import Generator

from dropbox import dropbox
from dropbox import common


class FileInfoBase():
    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def number(self) -> int:
        raise NotImplementedError

class SourceBase(metaclass=ABCMeta):
    def get_files(self) -> Generator[FileInfoBase, None, None]:
        raise NotImplementedError

    def download_file(self, file: FileInfoBase, destination_folder: str):
        raise NotImplementedError


class DropBoxFile(FileInfoBase):
    def __init__(self, filename, path_display, job_number):
        self.filename = filename
        self.path_display = path_display
        self.job_number = job_number

    @property
    def path(self) -> str:
        return self.path_display

    @property
    def name(self) -> str:
        return self.filename


class Job():
    def __init__(self, number, folder):
        self.number = number
        self.folder = folder

job_pattern = re.compile(r"j(?P<job>\d+)_", re.I)
def get_job_id(name):
    m = job_pattern.search(name)
    if m is None:
        return None
    return int(m['job'])

class DropBoxSource(SourceBase):
    def __init__(self, access_token: str, start_folder: str):
        db = dropbox.Dropbox(access_token)
        act = db.users_get_current_account()
        self._dbx = db.with_path_root(common.PathRoot.namespace_id(act.root_info.root_namespace_id))        
        self._start_folder = start_folder

    def _decode_exception(self, e: Exception):
        if isinstance(e, dropbox.ApiError):
            if e.user_message_text is not None:
                raise Exception(f"Dropbox error: {e.user_message_text}")
            if isinstance(e.error, dropbox.files.ListFolderError):
                if e.error.is_path():
                    raise Exception(f"Dropbox folder error: {str(e.error.get_path())}")
        raise Exception(f"Dropbox error. No description provided: {str(e)}")

    def list_folders(self, path, recursive):
        entries = []
        r = self._dbx.files_list_folder(path, recursive=recursive, include_deleted=False, include_mounted_folders=False)
        entries.extend(r.entries)
        while r.has_more:
            r = self._dbx.files_list_folder_continue(r.cursor)
            entries.extend(r.entries)
        return entries

    def get_job_folders(self):
        job_min = int(os.getenv('JOB_MIN','1'))
        job_max = int(os.getenv('JOB_MAX','1000'))
        logging.info(f'Listing job folders between {job_min} and {job_max}')
        all_files = self.list_folders(self._start_folder, False)
        logging.info(f'Identified {len(all_files)} level 1 files and folders')
        folders = []
        for f in all_files:
            job = get_job_id(f.name)
            if job is not None and (job>=job_min) and (job<job_max):
                folders.append(Job(job, f))
        logging.info(f'Identified {len(folders)} between {job_min} and {job_max}')
        return folders

    def get_files(self) -> Generator[FileInfoBase, None, None]:
        try:
            for f in self.get_job_folders():
                logging.debug(f'Enumerating files in {f.folder.path_display}')
                r = self._dbx.files_list_folder(f.folder.path_display, recursive=True)
                for e in r.entries:
                    yield DropBoxFile(e.name, e.path_display, f.number)
                while r.has_more:
                    r = self._dbx.files_list_folder_continue(r.cursor)
                    for e in r.entries:
                        yield DropBoxFile(e.name, e.path_display, f.number)

        except dropbox.ApiError as e:
            raise self._decode_exception(e)

    def download_file(self, file_to_download: DropBoxFile, destination_name: str):
        try:
            with open(destination_name, "wb") as file:
                metadata, res = self._dbx.files_download(path=file_to_download.path)
                file.write(res.content)
        except dropbox.ApiError as e:
            raise self._decode_exception(e)