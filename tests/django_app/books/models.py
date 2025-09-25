from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.core.validators import MinValueValidator, MaxValueValidator
import uuid
from decimal import Decimal


class Author(models.Model):
    """Author model to test foreign key relationships"""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200)
    bio = models.TextField(blank=True)
    birth_date = models.DateField(null=True, blank=True)
    nationality = models.CharField(max_length=100, blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class Publisher(models.Model):
    """Publisher model to test many-to-many relationships"""
    name = models.CharField(max_length=200)
    founded_year = models.IntegerField(null=True, blank=True)
    headquarters = models.CharField(max_length=200, blank=True)
    website = models.URLField(blank=True)

    def __str__(self):
        return self.name


class Genre(models.Model):
    """Genre model for many-to-many relationship testing"""
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    parent_genre = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True)

    def __str__(self):
        return self.name


class Book(models.Model):
    """Comprehensive book model testing many PostgreSQL features"""

    # Primary key and basic fields
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(max_length=200)
    subtitle = models.CharField(max_length=300, blank=True)
    isbn = models.CharField(max_length=13, unique=True)
    isbn10 = models.CharField(max_length=10, blank=True, null=True)

    # Text fields of various types
    description = models.TextField(blank=True, null=True)
    summary = models.CharField(max_length=1000, blank=True)
    excerpt = models.TextField(blank=True)

    # Numeric fields
    price = models.DecimalField(max_digits=10, decimal_places=2)
    discount_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    pages = models.IntegerField(validators=[MinValueValidator(1), MaxValueValidator(10000)])
    weight_grams = models.FloatField(null=True, blank=True)
    rating = models.DecimalField(max_digits=3, decimal_places=2,
                               validators=[MinValueValidator(Decimal('0.00')), MaxValueValidator(Decimal('5.00'))],
                               null=True, blank=True)

    # Date and time fields
    publication_date = models.DateField()
    first_published = models.DateField(null=True, blank=True)
    last_reprint = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Boolean fields
    is_available = models.BooleanField(default=True)
    is_featured = models.BooleanField(default=False)
    is_bestseller = models.BooleanField(default=False)
    has_ebook = models.BooleanField(default=False)
    has_audiobook = models.BooleanField(default=False)

    # PostgreSQL-specific array fields
    tags = ArrayField(models.CharField(max_length=50), size=20, blank=True, default=list)
    languages = ArrayField(models.CharField(max_length=10), size=5, blank=True, default=list)
    formats = ArrayField(models.CharField(max_length=20), size=10, blank=True, default=list)
    awards = ArrayField(models.CharField(max_length=200), blank=True, default=list)

    # Different array types
    chapter_page_counts = ArrayField(models.IntegerField(), blank=True, default=list)
    review_scores = ArrayField(models.DecimalField(max_digits=3, decimal_places=1), blank=True, default=list)

    # JSON field for complex data
    metadata = models.JSONField(default=dict, blank=True)
    reviews_data = models.JSONField(default=dict, blank=True)
    sales_data = models.JSONField(default=dict, blank=True)

    # Foreign key relationships
    primary_author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name='authored_books')
    co_authors = models.ManyToManyField(Author, blank=True, related_name='co_authored_books')
    publisher = models.ForeignKey(Publisher, on_delete=models.SET_NULL, null=True, blank=True)
    genres = models.ManyToManyField(Genre, blank=True)

    # Choice field
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('review', 'Under Review'),
        ('published', 'Published'),
        ('out_of_print', 'Out of Print'),
        ('discontinued', 'Discontinued'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')

    CONDITION_CHOICES = [
        ('new', 'New'),
        ('like_new', 'Like New'),
        ('good', 'Good'),
        ('acceptable', 'Acceptable'),
        ('damaged', 'Damaged'),
    ]
    condition = models.CharField(max_length=20, choices=CONDITION_CHOICES, default='new')

    class Meta:
        ordering = ['-created_at', 'title']
        indexes = [
            models.Index(fields=['title']),
            models.Index(fields=['isbn']),
            models.Index(fields=['publication_date']),
            models.Index(fields=['price']),
            models.Index(fields=['is_available', 'status']),
        ]
        constraints = [
            models.CheckConstraint(check=models.Q(price__gte=0), name='books_price_positive'),
            models.CheckConstraint(check=models.Q(pages__gte=1), name='books_pages_positive'),
            models.UniqueConstraint(fields=['isbn'], name='books_unique_isbn'),
        ]

    def __str__(self):
        return f"{self.title} by {self.primary_author.name}"

    @property
    def effective_price(self):
        """Return discount price if available, otherwise regular price"""
        return self.discount_price if self.discount_price else self.price

    @property
    def has_discount(self):
        """Check if book has a discount"""
        return self.discount_price is not None and self.discount_price < self.price


class Review(models.Model):
    """Review model to test additional relationships and text search"""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    book = models.ForeignKey(Book, on_delete=models.CASCADE, related_name='reviews')
    reviewer_name = models.CharField(max_length=200)
    reviewer_email = models.EmailField()
    rating = models.IntegerField(validators=[MinValueValidator(1), MaxValueValidator(5)])
    title = models.CharField(max_length=300)
    content = models.TextField()
    is_verified_purchase = models.BooleanField(default=False)
    is_featured = models.BooleanField(default=False)
    helpful_votes = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # JSON field for additional data
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['book', 'rating']),
            models.Index(fields=['is_verified_purchase']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"Review of {self.book.title} by {self.reviewer_name}"


class BookInventory(models.Model):
    """Inventory model to test additional numeric types and constraints"""
    book = models.OneToOneField(Book, on_delete=models.CASCADE, related_name='inventory')
    quantity_in_stock = models.IntegerField(default=0)
    quantity_reserved = models.IntegerField(default=0)
    quantity_sold = models.BigIntegerField(default=0)
    reorder_level = models.IntegerField(default=10)
    max_stock_level = models.IntegerField(default=1000)
    cost_price = models.DecimalField(max_digits=10, decimal_places=2)
    last_restocked = models.DateTimeField(null=True, blank=True)

    # Array of warehouse locations
    warehouse_locations = ArrayField(models.CharField(max_length=50), blank=True, default=list)

    # JSON for complex inventory data
    supply_chain_data = models.JSONField(default=dict, blank=True)

    class Meta:
        constraints = [
            models.CheckConstraint(check=models.Q(quantity_in_stock__gte=0), name='inventory_stock_non_negative'),
            models.CheckConstraint(check=models.Q(quantity_reserved__gte=0), name='inventory_reserved_non_negative'),
            models.CheckConstraint(check=models.Q(reorder_level__gte=0), name='inventory_reorder_non_negative'),
        ]

    @property
    def available_quantity(self):
        return max(0, self.quantity_in_stock - self.quantity_reserved)

    def __str__(self):
        return f"Inventory for {self.book.title}"
