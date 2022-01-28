from django.urls import path
from weekly.views import ChannelTimeSeriesView

urlpatterns = [
    path('/channel/timeseries', ChannelTimeSeriesView.as_view()),
]
