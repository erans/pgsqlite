from rest_framework import serializers
from .models import Book, Author, Publisher, Genre, Review, BookInventory


class AuthorSerializer(serializers.ModelSerializer):
    authored_books_count = serializers.SerializerMethodField()

    class Meta:
        model = Author
        fields = [
            'id', 'name', 'bio', 'birth_date', 'nationality', 'is_active',
            'created_at', 'authored_books_count'
        ]
        read_only_fields = ['id', 'created_at']

    def get_authored_books_count(self, obj):
        return obj.authored_books.count()


class PublisherSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publisher
        fields = ['id', 'name', 'founded_year', 'headquarters', 'website']
        read_only_fields = ['id']


class GenreSerializer(serializers.ModelSerializer):
    parent_genre_name = serializers.CharField(source='parent_genre.name', read_only=True)
    subgenres = serializers.SerializerMethodField()

    class Meta:
        model = Genre
        fields = ['id', 'name', 'description', 'parent_genre', 'parent_genre_name', 'subgenres']
        read_only_fields = ['id']

    def get_subgenres(self, obj):
        subgenres = Genre.objects.filter(parent_genre=obj)
        return [{'id': sg.id, 'name': sg.name} for sg in subgenres]


class BookInventorySerializer(serializers.ModelSerializer):
    available_quantity = serializers.ReadOnlyField()

    class Meta:
        model = BookInventory
        fields = [
            'quantity_in_stock', 'quantity_reserved', 'quantity_sold',
            'reorder_level', 'max_stock_level', 'cost_price', 'last_restocked',
            'warehouse_locations', 'supply_chain_data', 'available_quantity'
        ]


class ReviewSerializer(serializers.ModelSerializer):
    class Meta:
        model = Review
        fields = [
            'id', 'book', 'reviewer_name', 'reviewer_email', 'rating', 'title',
            'content', 'is_verified_purchase', 'is_featured', 'helpful_votes',
            'metadata', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']

    def validate_rating(self, value):
        if not 1 <= value <= 5:
            raise serializers.ValidationError("Rating must be between 1 and 5")
        return value


class BookSerializer(serializers.ModelSerializer):
    primary_author_name = serializers.CharField(source='primary_author.name', read_only=True)
    co_authors_names = serializers.SerializerMethodField()
    publisher_name = serializers.CharField(source='publisher.name', read_only=True)
    genres_names = serializers.SerializerMethodField()
    effective_price = serializers.ReadOnlyField()
    has_discount = serializers.ReadOnlyField()
    reviews = ReviewSerializer(many=True, read_only=True)
    inventory = BookInventorySerializer(read_only=True)
    average_rating = serializers.SerializerMethodField()
    reviews_count = serializers.SerializerMethodField()

    class Meta:
        model = Book
        fields = [
            # Basic fields
            'id', 'title', 'subtitle', 'isbn', 'isbn10',

            # Text fields
            'description', 'summary', 'excerpt',

            # Numeric fields
            'price', 'discount_price', 'effective_price', 'has_discount',
            'pages', 'weight_grams', 'rating',

            # Dates
            'publication_date', 'first_published', 'last_reprint',
            'created_at', 'updated_at',

            # Booleans
            'is_available', 'is_featured', 'is_bestseller', 'has_ebook', 'has_audiobook',

            # Arrays
            'tags', 'languages', 'formats', 'awards',
            'chapter_page_counts', 'review_scores',

            # JSON
            'metadata', 'reviews_data', 'sales_data',

            # Relationships
            'primary_author', 'primary_author_name',
            'co_authors', 'co_authors_names',
            'publisher', 'publisher_name',
            'genres', 'genres_names',

            # Status
            'status', 'condition',

            # Related data
            'reviews', 'inventory', 'average_rating', 'reviews_count'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'effective_price', 'has_discount']

    def get_co_authors_names(self, obj):
        return [author.name for author in obj.co_authors.all()]

    def get_genres_names(self, obj):
        return [genre.name for genre in obj.genres.all()]

    def get_average_rating(self, obj):
        reviews = obj.reviews.all()
        if reviews:
            return sum(review.rating for review in reviews) / len(reviews)
        return None

    def get_reviews_count(self, obj):
        return obj.reviews.count()

    def validate_isbn(self, value):
        """Ensure ISBN is exactly 13 characters and numeric"""
        if not value.isdigit() or len(value) != 13:
            raise serializers.ValidationError("ISBN must be exactly 13 digits")
        return value

    def validate_isbn10(self, value):
        """Validate ISBN-10 format if provided"""
        if value and (not value.replace('X', '').replace('x', '').isdigit() or len(value) != 10):
            raise serializers.ValidationError("ISBN-10 must be exactly 10 characters")
        return value

    def validate_price(self, value):
        """Ensure price is positive"""
        if value < 0:
            raise serializers.ValidationError("Price must be positive")
        return value

    def validate_pages(self, value):
        """Ensure pages count is reasonable"""
        if not 1 <= value <= 10000:
            raise serializers.ValidationError("Pages must be between 1 and 10000")
        return value

    def validate_languages(self, value):
        """Validate language codes"""
        valid_languages = ['en', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'zh', 'ja', 'ko']
        for lang in value:
            if lang not in valid_languages:
                raise serializers.ValidationError(f"Invalid language code: {lang}")
        return value


class BookListSerializer(serializers.ModelSerializer):
    """Simplified serializer for list views"""
    primary_author_name = serializers.CharField(source='primary_author.name', read_only=True)
    effective_price = serializers.ReadOnlyField()
    average_rating = serializers.SerializerMethodField()

    class Meta:
        model = Book
        fields = [
            'id', 'title', 'primary_author_name', 'price', 'effective_price',
            'publication_date', 'is_available', 'status', 'tags',
            'average_rating', 'created_at'
        ]

    def get_average_rating(self, obj):
        reviews = obj.reviews.all()
        if reviews:
            return round(sum(review.rating for review in reviews) / len(reviews), 2)
        return None