"""Database enum definitions."""

from enum import StrEnum


class PaymentStatus(StrEnum):
    UNPAID = "unpaid"
    PARTIALLY_PAID = "partially_paid"
    PAID = "paid"
    LATE = "late"
