from django.urls import path
from weekly.views import (SubcategoryTimeSeriesView,
                          ChannelTimeSeriesView)

urlpatterns = [
    path('/subcategory/timeseries', SubcategoryTimeSeriesView.as_view()),
    path('/channel/timeseries', ChannelTimeSeriesView.as_view()),
]
