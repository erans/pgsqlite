from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    BookViewSet, AuthorViewSet, PublisherViewSet,
    GenreViewSet, ReviewViewSet
)

router = DefaultRouter()
router.register(r'books', BookViewSet)
router.register(r'authors', AuthorViewSet)
router.register(r'publishers', PublisherViewSet)
router.register(r'genres', GenreViewSet)
router.register(r'reviews', ReviewViewSet)

urlpatterns = [
    path('api/', include(router.urls)),
]