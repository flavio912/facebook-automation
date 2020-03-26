from abc import ABCMeta

class StorageBase(metaclass=ABCMeta):
    def create_session_id(self) -> int:
        raise NotImplementedError

    def session_completed(self, id: int):
        raise NotImplementedError

    def session_completed_error(self, id: int, err: str):
        raise NotImplementedError

    def create_video(self, session_id: int, video_id: str, name: str, original_path: str, status=None):
        raise NotImplementedError

    def update_video_status(self, id: str, new_status: str):
        raise NotImplementedError
