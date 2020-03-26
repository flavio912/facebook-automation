from django.urls import path
from . import views

urlpatterns = [
    path('', views.sessions_list, name='sessions_list'),
    path('session/<int:id>', views.view_session, name='view_session')
]