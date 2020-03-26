from django.db.models import Count
from django.shortcuts import render

from .models import ScanningSession, UploadedFile


def sessions_list(request):
    sessions = ScanningSession.objects.all().annotate(videos_count=Count("uploadedfile")).order_by('-pk')
    return render(request, 'sessions_list.html', {"sessions":sessions})

def view_session(request, id):
    try:
        session = ScanningSession.objects.get(pk=id)
        videos = UploadedFile.objects.filter(session=session).order_by('-pk')
        return render(request, 'view_session.html', {"session":session,"videos":videos,"videos_len":len(videos)})
    except ScanningSession.DoesNotExist:
        return render(request, 'not_found.html')
