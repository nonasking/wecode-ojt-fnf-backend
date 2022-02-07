from django.urls import path

from search.views import SearchCountCompetitorTimeSeriesView, SearchCountTableView, SearchCountTimeSeriesOverallView

urlpatterns = [
    path('/timeseries/competitors',SearchCountCompetitorTimeSeriesView.as_view()),
    path('/table/<str:term>',SearchCountTableView.as_view()),
    path('/timeseries/overall',SearchCountTimeSeriesOverallView.as_view()),
]