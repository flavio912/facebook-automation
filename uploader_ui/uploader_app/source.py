import os
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


class SourceBase(metaclass=ABCMeta):
    def get_files(self) -> Generator[FileInfoBase, None, None]:
        raise NotImplementedError

    def download_file(self, file: FileInfoBase, destination_folder: str):
        raise NotImplementedError


class DropBoxFile(FileInfoBase):
    def __init__(self, filename, path_display):
        self.filename = filename
        self.path_display = path_display

    @property
    def path(self) -> str:
        return self.path_display

    @property
    def name(self) -> str:
        return self.filename


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

    def get_files(self) -> Generator[FileInfoBase, None, None]:
        try:
            r = self._dbx.files_list_folder(self._start_folder, recursive=True)
            cursor = r.cursor
            for e in r.entries:
                yield DropBoxFile(e.name, e.path_display)
            while r.has_more:
                r = self._dbx.files_list_folder_continue(cursor)
                for e in r.entries:
                    yield DropBoxFile(e.name, e.path_display)
        except dropbox.ApiError as e:
            raise self._decode_exception(e)

    def download_file(self, file_to_download: DropBoxFile, destination_name: str):
        try:
            with open(destination_name, "wb") as file:
                metadata, res = self._dbx.files_download(path=file_to_download.path)
                file.write(res.content)
        except dropbox.ApiError as e:
            raise self._decode_exception(e)