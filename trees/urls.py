from django.urls import path
from trees.views import CategoryTreeView, StyleRankingTreeView

urlpatterns = [
    path('', CategoryTreeView.as_view()),
    path('/style-ranking', StyleRankingTreeView.as_view()),
]