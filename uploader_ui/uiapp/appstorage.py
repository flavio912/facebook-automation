from uploader_app.storage import StorageBase
from .models import ScanningSession, UploadedFile


class DjangoStorage(StorageBase):
    def create_session_id(self) -> int:
        sess = ScanningSession()
        sess.status = 0
        sess.save()
        return sess.pk

    def session_completed(self, session_id: int):
        sess = ScanningSession.objects.get(pk=session_id)
        sess.status = 1
        sess.save()

    def session_completed_error(self, session_id: int, err: str):
        sess = ScanningSession.objects.get(pk=session_id)
        sess.status = 2
        sess.session_error = err
        sess.save()

    def create_video(self, session_id: int, video_id: str, name: str, original_path: str, status=None):
        sess = ScanningSession.objects.get(pk=session_id)
        f = UploadedFile()
        f.name = name
        f.file_id = video_id
        f.original_path = original_path
        f.session = sess
        f.status = status
        f.save()

    def update_video_status(self, video_id: str, new_status: str):
        try:
            v = UploadedFile.objects.get(file_id=video_id)
            v.status = new_status
            v.save()
        except:
            pass


