from datetime import datetime
from decimal import Decimal
from loguru import logger
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from zoneinfo import ZoneInfo
from billing.models import Account, AccountEntry
from legacy.nda import transactions
from legacy.event import SimpleEvent
import glob
import os

class Command(BaseCommand):
    help = 'Import bank transactions from .nda files'

    def add_arguments(self, parser):
        parser.add_argument('filename_pattern', type=str, help='Path/pattern for .nda files (wildcards allowed)')

    def handle(self, *args, **options):
        pattern = options['filename_pattern']
        total_count = 0
        total_failed = 0

        # Get list of files matching the pattern
        files = glob.glob(pattern)
        if not files:
            logger.error(f"No files found matching pattern: {pattern}")
            return

        logger.info(f"Found {len(files)} files to process")

        # Hardcoded account numbers as in the original
        account_numbers = ['FI2413093000112458']
        
        if not account_numbers:
            logger.error("There are no bank accounts provided for NDA import")
            return
        else:
            logger.debug(f"Using provided bank accounts: {account_numbers}")

        # Process each file
        with transaction.atomic():
            for filename in files:
                logger.debug(f"Starting NDA transaction import from {filename}")
                count = 0
                failed = 0

                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        # Get transaction iterator from nda module
                        txn_reader = transactions(f)
                        logger.debug("Successfully opened and parsed NDA file")

                        # Generate SimpleEvents from transactions
                        logger.debug("Starting transaction filtering and conversion to events")
                        events = SimpleEvent.generate_from_nda(
                            txn_reader,
                            account_numbers,
                            lambda txn: (
                                txn.cents > 0 and  # Only positive amounts
                                txn.ref and  # Must have reference number
                                len(txn.ref) in (4,6)  # Valid reference number length
                            )
                        )

                        # Process each event within a transaction
                        
                        for event in events:
                            try:
                                # Find account by account ID
                                try:
                                    account = Account.objects.get(id=event.account_id)
                                except Account.DoesNotExist:
                                    logger.warning(
                                        f"Skipping transaction: Account with ID {event.account_id} "
                                        f"not found (amount: {event.amount}, date: {event.date})"
                                    )
                                    failed += 1
                                    continue

                                # Create AccountEntry
                                entry = AccountEntry.objects.create(
                                    account=account,
                                    date=timezone.make_aware(
                                        datetime.combine(event.date, datetime.min.time()),  # Convert date to datetime
                                        timezone=ZoneInfo('Europe/Helsinki')
                                    ),
                                    amount=event.amount,
                                    description="Maksu"
                                )
                                count += 1
                                logger.debug(
                                    f"Added transaction: {entry.date} | {entry.amount} | "
                                    f"{entry.description} | ref: {entry.account_id}"
                                )

                            except Exception as e:
                                logger.error(
                                    f"Error processing event: {str(e)} "
                                    f"(date: {event.date} | {event.amount} | Maksu | "
                                    f"ref: {event.account_id})"
                                )
                                failed += 1

                except Exception as e:
                    error_msg = f"Error reading NDA file {filename}: {str(e)}"
                    logger.exception(error_msg)
                    continue  # Continue with next file instead of raising

            logger.info(f"File {os.path.basename(filename)} completed: {count} transactions imported, {failed} failed")
            total_count += count
            total_failed += failed

        logger.info(f"All NDA imports completed: {total_count} total transactions imported, {total_failed} total failed")