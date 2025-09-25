from rest_framework import viewsets, status, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Q, Avg, Count, Min, Max, Sum
from django.db.models.functions import Extract
from django.utils import timezone
from datetime import datetime, timedelta
from .models import Book, Author, Publisher, Genre, Review, BookInventory
from .serializers import (
    BookSerializer, BookListSerializer, AuthorSerializer, PublisherSerializer,
    GenreSerializer, ReviewSerializer, BookInventorySerializer
)


class AuthorViewSet(viewsets.ModelViewSet):
    """ViewSet for Author model"""
    queryset = Author.objects.all()
    serializer_class = AuthorSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'bio', 'nationality']
    ordering_fields = ['name', 'birth_date', 'created_at']
    ordering = ['name']

    def get_queryset(self):
        queryset = Author.objects.all()

        # Filter by nationality
        nationality = self.request.query_params.get('nationality')
        if nationality:
            queryset = queryset.filter(nationality__icontains=nationality)

        # Filter by active status
        is_active = self.request.query_params.get('active')
        if is_active is not None:
            active_val = is_active.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(is_active=active_val)

        return queryset

    @action(detail=True, methods=['get'])
    def books(self, request, pk=None):
        """Get all books by this author"""
        author = self.get_object()
        books = Book.objects.filter(
            Q(primary_author=author) | Q(co_authors=author)
        ).distinct()
        serializer = BookListSerializer(books, many=True)
        return Response(serializer.data)


class PublisherViewSet(viewsets.ModelViewSet):
    """ViewSet for Publisher model"""
    queryset = Publisher.objects.all()
    serializer_class = PublisherSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'headquarters']
    ordering_fields = ['name', 'founded_year']
    ordering = ['name']


class GenreViewSet(viewsets.ModelViewSet):
    """ViewSet for Genre model"""
    queryset = Genre.objects.all()
    serializer_class = GenreSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = ['name', 'description']


class ReviewViewSet(viewsets.ModelViewSet):
    """ViewSet for Review model"""
    queryset = Review.objects.all()
    serializer_class = ReviewSerializer
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ['created_at', 'rating', 'helpful_votes']
    ordering = ['-created_at']

    def get_queryset(self):
        queryset = Review.objects.all()

        # Filter by book
        book_id = self.request.query_params.get('book')
        if book_id:
            queryset = queryset.filter(book_id=book_id)

        # Filter by rating
        rating = self.request.query_params.get('rating')
        if rating:
            queryset = queryset.filter(rating=rating)

        # Filter by verified purchases
        verified = self.request.query_params.get('verified')
        if verified is not None:
            is_verified = verified.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(is_verified_purchase=is_verified)

        return queryset


class BookViewSet(viewsets.ModelViewSet):
    """
    Comprehensive viewset for Book model with advanced filtering and actions
    """
    queryset = Book.objects.select_related('primary_author', 'publisher').prefetch_related(
        'co_authors', 'genres', 'reviews', 'inventory'
    )
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['title', 'subtitle', 'description', 'primary_author__name']
    ordering_fields = ['title', 'price', 'publication_date', 'created_at', 'rating', 'pages']
    ordering = ['-created_at']

    def get_serializer_class(self):
        if self.action == 'list':
            return BookListSerializer
        return BookSerializer

    def get_queryset(self):
        """Advanced filtering capabilities"""
        queryset = self.queryset

        # Price range filtering
        min_price = self.request.query_params.get('min_price')
        max_price = self.request.query_params.get('max_price')
        if min_price:
            queryset = queryset.filter(price__gte=min_price)
        if max_price:
            queryset = queryset.filter(price__lte=max_price)

        # Date range filtering
        start_date = self.request.query_params.get('published_after')
        end_date = self.request.query_params.get('published_before')
        if start_date:
            queryset = queryset.filter(publication_date__gte=start_date)
        if end_date:
            queryset = queryset.filter(publication_date__lte=end_date)

        # Boolean filters
        available = self.request.query_params.get('available')
        if available is not None:
            is_available = available.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(is_available=is_available)

        featured = self.request.query_params.get('featured')
        if featured is not None:
            is_featured = featured.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(is_featured=is_featured)

        bestseller = self.request.query_params.get('bestseller')
        if bestseller is not None:
            is_bestseller = bestseller.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(is_bestseller=is_bestseller)

        # Status filtering
        status_filter = self.request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)

        # Genre filtering
        genre = self.request.query_params.get('genre')
        if genre:
            queryset = queryset.filter(genres__name__icontains=genre)

        # Publisher filtering
        publisher = self.request.query_params.get('publisher')
        if publisher:
            queryset = queryset.filter(publisher__name__icontains=publisher)

        # Author filtering (primary or co-author)
        author = self.request.query_params.get('author')
        if author:
            queryset = queryset.filter(
                Q(primary_author__name__icontains=author) |
                Q(co_authors__name__icontains=author)
            ).distinct()

        # Array field filtering
        tag = self.request.query_params.get('tag')
        if tag:
            queryset = queryset.filter(tags__contains=[tag])

        language = self.request.query_params.get('language')
        if language:
            queryset = queryset.filter(languages__contains=[language])

        format_type = self.request.query_params.get('format')
        if format_type:
            queryset = queryset.filter(formats__contains=[format_type])

        # Pages range filtering
        min_pages = self.request.query_params.get('min_pages')
        max_pages = self.request.query_params.get('max_pages')
        if min_pages:
            queryset = queryset.filter(pages__gte=min_pages)
        if max_pages:
            queryset = queryset.filter(pages__lte=max_pages)

        # Full-text search across multiple fields
        q = self.request.query_params.get('q')
        if q:
            queryset = queryset.filter(
                Q(title__icontains=q) |
                Q(subtitle__icontains=q) |
                Q(description__icontains=q) |
                Q(summary__icontains=q) |
                Q(primary_author__name__icontains=q) |
                Q(publisher__name__icontains=q)
            ).distinct()

        return queryset

    @action(detail=False, methods=['get'])
    def statistics(self, request):
        """Get comprehensive statistics about books"""
        queryset = self.get_queryset()

        stats = {
            'total_books': queryset.count(),
            'available_books': queryset.filter(is_available=True).count(),
            'featured_books': queryset.filter(is_featured=True).count(),
            'bestsellers': queryset.filter(is_bestseller=True).count(),
            'avg_price': queryset.aggregate(Avg('price'))['price__avg'],
            'min_price': queryset.aggregate(Min('price'))['price__min'],
            'max_price': queryset.aggregate(Max('price'))['price__max'],
            'avg_pages': queryset.aggregate(Avg('pages'))['pages__avg'],
            'total_authors': Author.objects.count(),
            'total_publishers': Publisher.objects.count(),
            'total_genres': Genre.objects.count(),
            'total_reviews': Review.objects.count(),
            'avg_rating': Review.objects.aggregate(Avg('rating'))['rating__avg'],
        }

        # Publication year statistics
        year_stats = queryset.annotate(
            year=Extract('publication_date', 'year')
        ).values('year').annotate(
            count=Count('id')
        ).order_by('-year')[:10]

        stats['publications_by_year'] = list(year_stats)

        # Top genres
        genre_stats = Genre.objects.annotate(
            book_count=Count('book')
        ).order_by('-book_count')[:10]

        stats['top_genres'] = [
            {'name': g.name, 'count': g.book_count} for g in genre_stats
        ]

        return Response(stats)

    @action(detail=False, methods=['get'])
    def by_author(self, request):
        """Get books grouped by author"""
        queryset = self.get_queryset()
        author_name = request.query_params.get('author_name')

        if author_name:
            # Get books by specific author
            books = queryset.filter(primary_author__name__icontains=author_name)
            serializer = BookListSerializer(books, many=True)
            return Response(serializer.data)
        else:
            # Group books by author
            authors = Author.objects.prefetch_related('authored_books')
            result = {}
            for author in authors:
                books = author.authored_books.filter(id__in=queryset)
                if books.exists():
                    result[author.name] = BookListSerializer(books, many=True).data
            return Response(result)

    @action(detail=False, methods=['get'])
    def by_genre(self, request):
        """Get books grouped by genre"""
        genre_stats = Genre.objects.prefetch_related('book_set').annotate(
            book_count=Count('book')
        ).filter(book_count__gt=0)

        result = {}
        for genre in genre_stats:
            books = genre.book_set.filter(id__in=self.get_queryset())
            if books.exists():
                result[genre.name] = {
                    'count': books.count(),
                    'books': BookListSerializer(books[:10], many=True).data  # Limit to 10 books per genre
                }

        return Response(result)

    @action(detail=False, methods=['get'])
    def search_advanced(self, request):
        """Advanced search with complex queries"""
        queryset = self.get_queryset()

        # Search in JSON fields
        json_search = request.query_params.get('json_search')
        if json_search:
            queryset = queryset.filter(
                Q(metadata__icontains=json_search) |
                Q(reviews_data__icontains=json_search) |
                Q(sales_data__icontains=json_search)
            )

        # Array operations
        has_all_tags = request.query_params.getlist('must_have_tags')
        if has_all_tags:
            for tag in has_all_tags:
                queryset = queryset.filter(tags__contains=[tag])

        has_any_tags = request.query_params.getlist('any_tags')
        if has_any_tags:
            tag_q = Q()
            for tag in has_any_tags:
                tag_q |= Q(tags__contains=[tag])
            queryset = queryset.filter(tag_q)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['post'])
    def add_tag(self, request, pk=None):
        """Add a tag to a book"""
        book = self.get_object()
        tag = request.data.get('tag')

        if not tag:
            return Response({'error': 'Tag is required'}, status=status.HTTP_400_BAD_REQUEST)

        if tag not in book.tags:
            book.tags.append(tag)
            book.save()
            return Response({'message': f'Tag "{tag}" added', 'tags': book.tags})
        else:
            return Response({'error': 'Tag already exists'}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['delete'])
    def remove_tag(self, request, pk=None):
        """Remove a tag from a book"""
        book = self.get_object()
        tag = request.data.get('tag')

        if not tag:
            return Response({'error': 'Tag is required'}, status=status.HTTP_400_BAD_REQUEST)

        if tag in book.tags:
            book.tags.remove(tag)
            book.save()
            return Response({'message': f'Tag "{tag}" removed', 'tags': book.tags})
        else:
            return Response({'error': 'Tag not found'}, status=status.HTTP_404_NOT_FOUND)

    @action(detail=True, methods=['post'])
    def update_metadata(self, request, pk=None):
        """Update JSON metadata"""
        book = self.get_object()
        metadata_updates = request.data.get('metadata', {})

        # Merge with existing metadata
        book.metadata.update(metadata_updates)
        book.save()

        return Response({'message': 'Metadata updated', 'metadata': book.metadata})

    @action(detail=True, methods=['get'])
    def related_books(self, request, pk=None):
        """Get books related to this one (same author, genre, or publisher)"""
        book = self.get_object()

        # Find related books by various criteria
        related = Book.objects.exclude(id=book.id).filter(
            Q(primary_author=book.primary_author) |
            Q(publisher=book.publisher) |
            Q(genres__in=book.genres.all())
        ).distinct()[:10]

        serializer = BookListSerializer(related, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['post'])
    def mark_featured(self, request, pk=None):
        """Mark/unmark book as featured"""
        book = self.get_object()
        featured = request.data.get('featured', True)

        book.is_featured = featured
        book.save()

        return Response({
            'message': f'Book {"marked" if featured else "unmarked"} as featured',
            'is_featured': book.is_featured
        })
