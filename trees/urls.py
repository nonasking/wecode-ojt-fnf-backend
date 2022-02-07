from django.urls import path
from trees.views import CategoryTreeView

urlpatterns = [
    path('', CategoryTreeView.as_view()),
]