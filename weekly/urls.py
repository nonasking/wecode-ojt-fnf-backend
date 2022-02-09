from django.urls import path

from weekly.views.channel import (ChannelTimeSeriesView,
                                  ChannelTableView)
from weekly.views.subcategory import (SubcategoryTimeSeriesView,
                                      SubcategoryTableView)
from weekly.views.domain import (DomainTimeSeriesView,
                                 DomainTableView)
from weekly.views.distribution import (DistributionTimeSeriesView,
                                       DistributionTableView)
from weekly.views.style import StyleSaleView
urlpatterns = [
    path('/subcategory/timeseries', SubcategoryTimeSeriesView.as_view()),
    path('/subcategory/table', SubcategoryTableView.as_view()),
    path('/domain/timeseries', DomainTimeSeriesView.as_view()),
    path('/domain/table', DomainTableView.as_view()),
    path('/style/table', StyleSaleView.as_view()),
    path('/channel/timeseries', ChannelTimeSeriesView.as_view()),
    path('/channel/table', ChannelTableView.as_view()),
    path('/distribution/timeseries', DistributionTimeSeriesView.as_view()),
    path('/distribution/table', DistributionTableView.as_view()),
]
