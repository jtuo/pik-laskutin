from django.db import models
from django.core.exceptions import ValidationError
from django.utils import timezone
from decimal import Decimal, ROUND_HALF_UP
from django.db.models import Sum

class Account(models.Model):
    id = models.CharField(max_length=20, primary_key=True)  # PIK reference
    member = models.ForeignKey(
        'members.Member',
        on_delete=models.CASCADE,
        related_name='accounts'
    )
    name = models.CharField(max_length=100, blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'accounts'

    def __str__(self):
        return f"<Account {self.id}: {self.name}>"

class AccountEntry(models.Model):
    date = models.DateTimeField(db_index=True)
    account = models.ForeignKey(
        Account,
        on_delete=models.CASCADE,
        related_name='entries',
        null=False
    )
    description = models.TextField()
    amount = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        help_text="Positive = charge, Negative = payment/credit"
    )
    additive = models.BooleanField(default=True)
    event = models.ForeignKey(
        'operations.BaseEvent',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='account_entries'
    )
    invoice = models.ForeignKey(
        'Invoice',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='entries'
    )
    ledger_account_id = models.CharField(
        max_length=20,
        null=True,
        blank=True,
        help_text="For mapping to external accounting system"
    )
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = 'account_entries'
        verbose_name = 'Entry'
        verbose_name_plural = 'Entries'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.amount is not None:
            self.amount = Decimal(str(self.amount)).quantize(
                Decimal('.01'),
                rounding=ROUND_HALF_UP
            )

    def clean(self):
        if self.amount is not None:
            # Double check the amount is properly quantized
            quantized = Decimal(str(self.amount)).quantize(
                Decimal('.01'),
                rounding=ROUND_HALF_UP
            )
            if self.amount != quantized:
                self.amount = quantized

    @property
    def is_modifiable(self):
        if not self.event:
            return True
        return self.event.type != 'invoice'

    @property
    def is_balance_correction(self):
        return self.force_balance is not None

    def __str__(self):
        return f"<AccountEntry {self.date}: {self.amount}>"

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

class Invoice(models.Model):
    class Status(models.TextChoices):
        DRAFT = 'draft', 'Draft'
        SENT = 'sent', 'Sent'
        PAID = 'paid', 'Paid'
        CANCELLED = 'cancelled', 'Cancelled'

    number = models.CharField(max_length=20, unique=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    account = models.ForeignKey(
        Account,
        on_delete=models.CASCADE,
        related_name='invoices'
    )
    due_date = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=10,
        choices=Status.choices,
        default=Status.DRAFT,
        db_index=True
    )
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'invoices'

    def clean(self):
        if not self.number:
            raise ValidationError("Invoice number cannot be empty")

    @property
    def total_amount(self):
        return self.entries.aggregate(
            total=Sum('amount')
        )['total'] or Decimal('0.00')

    @property
    def is_overdue(self):
        return (
            self.due_date is not None and
            self.status not in [self.Status.PAID, self.Status.CANCELLED] and
            timezone.now() > self.due_date
        )

    def can_be_sent(self):
        return (
            self.status == self.Status.DRAFT and
            self.entries.exists() and
            self.due_date is not None
        )

    def __str__(self):
        return f"<Invoice {self.number}: {self.status}>"

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)
