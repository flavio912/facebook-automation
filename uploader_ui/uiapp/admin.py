from django.contrib import admin

# Register your models here.
from .models import UploadedFile, ScanningSession


class ScanningSessionAdmin(admin.ModelAdmin):
    list_display = ('pk', 'created_at', 'last_modified_at', 'status', 'session_error')


class UploadedFileAdmin(admin.ModelAdmin):
    list_display = ('pk', 'created_at', 'original_path', 'file_id', 'status', 'name', 'session',)


admin.site.register(ScanningSession, ScanningSessionAdmin)
admin.site.register(UploadedFile, UploadedFileAdmin)
