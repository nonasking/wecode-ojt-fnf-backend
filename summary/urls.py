from django.urls import path

from summary.views.summaryacc import SalesSummaryAccView
from summary.views.accseason import SalesSummaryAccSesnView
from summary.views.salesweekly import WeeklySalesSummaryView

urlpatterns = [
    path('/sales-summary-acc', SalesSummaryAccView.as_view()),
    path('/sales-summary-acc-season', SalesSummaryAccSesnView.as_view()),
    path('/weekly-sales-summary', WeeklySalesSummaryView.as_view()),    
]
