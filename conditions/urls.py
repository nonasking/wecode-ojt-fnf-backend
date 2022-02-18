from django.urls import path

from conditions.views import SalesTrendView, WeeklyView, ChannelView

urlpatterns = [
    path('/sales-trend/<str:type>',SalesTrendView.as_view()),
    path('/weekly',WeeklyView.as_view()),
    path('/channel/<str:type>',ChannelView.as_view()),
]