import logging
import os
import time

from facebook_business import FacebookAdsApi
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.exceptions import FacebookBadObjectError, FacebookRequestError

from .pattern import is_file_match
from .source import SourceBase, FileInfoBase
from .storage import StorageBase
from .uploader import UploaderBase

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
        self.templates = os.getenv('AD_CAMPAIGN_TEMPLATE_ID').split(',')

    def _handle_file(self, session_id: id, file: FileInfoBase):
        logging.debug(f"_handle_file session_id:{session_id} file.name={file.name} file.path={file.path}")

        # Check FileName format
        is_matched = is_file_match(file.name)
        # Upload module. NOT upload if video is already uploaded.
        if is_matched and self._uploader.should_be_uploaded(file.name):
            new_file = os.path.join(self._tmp_dir, file.name)
            logging.info(f"Downloading: {file.name}")

            self._source.download_file(file, new_file)
            logging.info(f"Successful download, uploading: {file.name}")

            uploaded = None;
            uploaded = self._uploader.upload(new_file)
            if os.path.exists(new_file):
                os.remove(new_file);
            if uploaded is not None:
                self._storage.create_video(session_id, str(uploaded.id), file.name, file.path)
                logging.info(f"Successful upload: {file.name}")
                return uploaded, None
            else:
                self._storage.create_video(session_id, uploaded.id, file.name, file.path, "error")
                logging.warning(f"Upload failed: {file.name}")
                return None, file
        else:
            logging.info(f"Skip: {file.name}")
            return None, None

    def _create_ad(self, session_id: id, file: FileInfoBase):
        logging.debug(f"_create_ad session_id:{session_id} file.name={file.name} file.path={file.path}")
        """
        Create Ad with uploaded video file using Template Campaign
        It's possible there will be more than one template campaign per ad account.
        So it should really be 0 to N campaign templates.
        Template IDs is defined by environment variable 'AD_CAMPAIGN_TEMPLATE_ID',
        'AD_CAMPAIGN_TEMPLATE_ID' defines 0-N templates by separating with comma ','
        3 Template definition example; AD_CAMPAIGN_TEMPLATE_ID=("23844416049080002,23844416049080002,23844416049080002")
        """

        logging.info(f'Processing:{file.path}')

        temp_file = os.path.join(self._tmp_dir, file.name)

        if os.path.exists(temp_file):
            os.remove(temp_file);

        for template in self.templates:
            if template != '':
                res = self._uploader.create_ad_with_duplicate(file.path, file.name, file.job_number, template)
                logging.debug(f'Create Ad with video: {file.path}, Template id:{template} -> {res}')

        return True

    def _do_index(self):
        self._uploader.index()
        return True

    def run(self):
        FacebookAdsApi.init(os.getenv('FB_GA_APPID'), os.getenv('FB_GA_APPKEY'), os.getenv('FB_GA_TOKEN'))
        logging.info("Indexing uploader...")
        if not self._do_index():
            logging.warning("index unsuccessful")
            return
        logging.info("Indexing done. Started scanning source")
        session_id = self._storage.create_session_id()

        parallelism = int(os.getenv('PARALLELISM', '1'))
        logging.info(f'Using {parallelism} threads to upload files')
        #pool = ThreadPool(parallelism)
        try:
            total_uploaded = []
            upload_names = []
            logging.info(f'Enumerating files in {self._source._start_folder}')
            all_files = list(self._source.get_files())
            logging.info(f'Found {len(all_files)} total files in {self._source._start_folder}')
            files = [f for f in all_files if is_file_match(f.filename)]
            logging.info(f'Found {len(files)} matching files in {self._source._start_folder}')

            while True:
                results = []
                for file in files:
                    upload_file = self._handle_file(session_id, file)
                    results.append(upload_file)
                    if upload_file[0] is not None:
                        file_with_name = upload_file[0]
                        file_with_name.name = file.name
                        upload_names.append(file_with_name)


                # results = pool.map(lambda file: self._handle_file(session_id, file), files)
                #pool.close()
                #pool.join()
                not_uploaded_files = list(filter(lambda x: x is not None, map(lambda x: x[1], results)))
                uploaded_videos = list(filter(lambda x: x is not None, map(lambda x: x[0], results)))
                total_uploaded += uploaded_videos
                if len(not_uploaded_files) > 0:
                    files = not_uploaded_files
                    logging.info(f"Not uploaded files: {len(not_uploaded_files)}. Retry")
                else:
                    break

            self._uploader.set_uploaded_videos(total_uploaded, upload_names)
            logging.info(f"{total_uploaded} files uploaded. Waiting for processing completion")
            for id, status in self._uploader.wait_all():
                self._storage.update_video_status(id, status)
            logging.info(f"File Upload is Done")

            # Create Ads with uploaded video by duplicating Template Campaigns(0-N)
            logging.info(f'Creating ADs with uploaded video files...')
            self._uploader.index_campaigns()
            self._uploader.index_template_adset_names(self.templates)
            logging.info(f'Available Template IDs: {self.templates}')
            for file in files:
                self._create_ad(session_id, file)
            logging.info(f"Create ADS is Done")

            self._storage.session_completed(session_id)
        except Exception as e:
            logging.exception(str(e))
            self._storage.session_completed_error(session_id, str(e))
