import logging
import os
import time

from facebook_business import FacebookAdsApi
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.exceptions import FacebookBadObjectError, FacebookRequestError

from .pattern import is_file_match
from .source import SourceBase, FileInfoBase
from .storage import StorageBase
from .uploader import UploaderBase, TooManyCallsError

from multiprocessing.dummy import Pool as ThreadPool

def get_parent_id(self):
    """
    There is no way no avoid parent_id using, since there is not such options for VideoUploader
    And also facebook SDK uses root logger so it's impossible to disable their warnings
    thats' why i'm redifined this method
    :param self:
    :return:
    """
    #warning_message = "parent_id is being deprecated."
    #logging.warning(warning_message)
    """Returns the object's parent's id."""
    return self._parent_id or FacebookAdsApi.get_default_account_id()

def get_parent_id_assured(self):
    """Returns the object's parent's fbid.
    Raises:
        FacebookBadObjectError if the object does not have a parent id.
    """
    #warning_message = "parent_id is being deprecated."
    #logging.warning(warning_message)
    if not self.get_parent_id():
        raise FacebookBadObjectError(
            "%s object needs a parent_id for this operation."
            % self.__class__.__name__,
        )

    return self.get_parent_id()

AbstractCrudObject.get_parent_id = get_parent_id
AbstractCrudObject.get_parent_id_assured = get_parent_id_assured

# start_folder = '/Jobs_Dev'
# start_folder = '/Jobs_Dev/J343_Survivor_RoofMoneyCount/Exports/T7/V2'

class Uploader:
    def __init__(self,
                 storage: StorageBase,
                 source: SourceBase,
                 uploader: UploaderBase,
                 tmp_dir: str
                 ):
        self._storage = storage
        self._source = source
        self._uploader = uploader
        self._tmp_dir = tmp_dir

    def _handle_file(self, session_id: id, file: FileInfoBase):
        logging.debug(f"_handle_file session_id:{session_id} file.name={file.name} file.path={file.path}")
        is_matched = is_file_match(file.name)
        if is_matched and self._uploader.should_be_uploaded(file.name):
            new_file = os.path.join(self._tmp_dir, file.name)
            logging.info(f"Downloading: {file.name}")
            self._source.download_file(file, new_file)
            logging.info(f"Successful download, uploading: {file.name}")
            uploaded = self._uploader.upload(new_file)
            if uploaded is not None:
                self._storage.create_video(session_id, uploaded.id, file.name, file.path)
                logging.info(f"Successful upload: {file.name}")
                return uploaded, None
            else:
                self._storage.create_video(session_id, uploaded.id, file.name, file.path, "error")
                logging.warning(f"Upload failed: {file.name}")
                return None, file
        else:
            logging.info(f"Skip: {file.name}")
            return None, None

    def _do_index(self):
        self._uploader.index()
        return True

    def run(self):
        logging.info("Indexing uploader...")
        if not self._do_index():
            logging.warning("index unsuccessful")
            return
        logging.info("Indexing done. Started scanning source")
        session_id = self._storage.create_session_id()

        parallelism = int(os.getenv('PARALLELISM', '5'))
        logging.info(f'Using {parallelism} threads to upload files')
        pool = ThreadPool(parallelism)
        try:
            total_uploaded = []
            logging.info(f'Enumerating files in {self._source._start_folder}')
            all_files = list(self._source.get_files())
            logging.info(f'Found {len(all_files)} total files in {self._source._start_folder}')
            files = [f for f in all_files if is_file_match(f.filename)]
            logging.info(f'Found {len(files)} matching files in {self._source._start_folder}')

            while True:
                results = pool.map(lambda file: self._handle_file(session_id, file), files)
                pool.close()
                pool.join()
                not_uploaded_files = list(filter(lambda x: x is not None, map(lambda x: x[1], results)))
                uploaded_videos = list(filter(lambda x: x is not None, map(lambda x: x[0], results)))
                total_uploaded += uploaded_videos
                if len(not_uploaded_files) > 0:
                    files = not_uploaded_files
                    logging.info(f"Not uploaded files: {len(not_uploaded_files)}. Retry")
                else:
                    break

            self._uploader.set_uploaded_videos(total_uploaded)
            logging.info(f"{total_uploaded} files uploaded. Waiting for processing completion")
            for id, status in self._uploader.wait_all():
                self._storage.update_video_status(id, status)
            logging.info(f"Done")
            self._storage.session_completed(session_id)
        except Exception as e:
            logging.exception(str(e))
            self._storage.session_completed_error(session_id, str(e))
