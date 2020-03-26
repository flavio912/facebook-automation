from django.db import models


class ScanningSession(models.Model):
    statuses = [
        (0, 'in-process'),
        (1, 'successfully completed'),
        (2, 'completed with error')
    ]

    created_at = models.DateTimeField(auto_now_add=True)
    last_modified_at = models.DateTimeField(auto_now=True)
    status = models.IntegerField(choices=statuses)
    session_error = models.CharField(max_length=255, null=True, default=None, blank=True)

    def __str__(self):
        return f"session #{self.pk }at {self.created_at}"


class UploadedFile(models.Model):
    original_path = models.CharField(max_length=1000)
    file_id = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=255, default=None, null=True, blank=True)
    name = models.CharField(max_length=500)
    session = models.ForeignKey(ScanningSession, on_delete=models.PROTECT)

    def __str__(self):
        return f"file #{self.file_id} created {self.created_at}"



