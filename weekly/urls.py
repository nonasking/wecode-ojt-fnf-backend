from django.urls import path
from weekly.views import TimeSeriesView

urlpatterns = [
    path('/time-series', TimeSeriesView.as_view()),
]
