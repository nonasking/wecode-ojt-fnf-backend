from django.urls import path
from top20.views import (Top20SummaryView,
                         Top20ListView)

urlpatterns = [
    path('/summary', Top20SummaryView.as_view()),
    path('/list', Top20ListView.as_view()),
]
